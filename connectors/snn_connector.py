#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
connectors/snn_connector.py

موصل SNN مُحسّن ليتكامل مع بقية مكونات النظام (paywall detector, metrics collector, deduper, orchestrator).
التعديلات الأساسية:
- يستدعي utils.paywall_detector.is_paywalled لإقصاء المحتوى المدفوع أو وضعه في topic للمراجعة اليدوية.
- يستدعي metrics.metrics_collector.fetch_metrics_for_url لجلب مقاييس أولية (views/likes/shares).
- يضيف حقول metadata قياسية متوافقة مع بقية النظام (fingerprint, source_host, fetched_at, connector_id).
- ينشر رسائل إلى Kafka topic عام `tech.raw` مع حقل `origin_connector` و`ingest_ts`.
- ينشر heartbeat إلى Kafka topic `tech.connectors.heartbeat` لتتبع حالة الموصل من Orchestrator.
- قابلية التشغيل كـone-shot أو loop مستمر، ويدعم تشغيل مؤقت عبر env var.
- كل الاستدعاءات الخارجية تتم عبر import آمن مع fallback لتشغيل PoC حتى قبل إرسال باقي الملفات.
"""

import os
import sys
import time
import json
import logging
import random
import socket
from typing import Generator, Dict, Optional
from urllib.parse import urlparse
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from confluent_kafka import Producer

# ---------------------------
# Attempt to import project helpers (optional; fallback safe stubs)
# ---------------------------
try:
    from utils.paywall_detector import is_paywalled  # returns (bool, details)
except Exception:
    def is_paywalled(url: str):
        # fallback: assume not paywalled (PoC mode)
        return (False, {"reason": "paywall_detector_not_available"})

try:
    from metrics.metrics_collector import fetch_metrics_for_url
except Exception:
    def fetch_metrics_for_url(url: str):
        return {"url": url, "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "views": None, "likes": None, "shares": None, "method": "fallback"}

try:
    # optional source registry/discovery helper
    from connectors.source_discovery import register_source_if_new
except Exception:
    def register_source_if_new(domain: str, meta: Dict = None):
        # noop fallback
        return False

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "tech.raw")
KAFKA_HEARTBEAT_TOPIC = os.getenv("KAFKA_HEARTBEAT_TOPIC", "tech.connectors.heartbeat")
SNN_RSS_URL = os.getenv("SNN_RSS_URL", "https://snn.example/rss")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
CONNECTOR_ID = os.getenv("CONNECTOR_ID", "snn_connector_v1")
USER_AGENT = os.getenv("CONNECTOR_USER_AGENT", "MediaIntelBot/1.0 (+https://example.com)")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.5"))
MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "200"))
ENABLE_SCRAPE_FALLBACK = os.getenv("ENABLE_SCRAPE_FALLBACK", "true").lower() == "true"
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "60"))  # seconds

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(
    level=os.getenv("CONNECTOR_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [snn_connector] %(message)s"
)
logger = logging.getLogger("snn_connector")

# ---------------------------
# Kafka Producer setup
# ---------------------------
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "socket.timeout.ms": 10000,
    "message.send.max.retries": 3,
}
producer = Producer(producer_conf)

# ---------------------------
# Helpers
# ---------------------------
def is_allowed_by_robots(url: str, user_agent: str = USER_AGENT) -> bool:
    """تحقق من robots.txt للمجال إن أمكن"""
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(user_agent, url)
        logger.debug("robots.txt check for %s -> %s", url, allowed)
        return allowed
    except Exception as e:
        logger.warning("robots.txt check failed (%s). Defaulting to allow. Error: %s", url, e)
        return True

def safe_request_get(url: str, headers: Dict[str, str], timeout: int = REQUEST_TIMEOUT) -> Optional[requests.Response]:
    """طلب HTTP آمن مع retry وexponential backoff"""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            attempt += 1
            sleep_for = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.5)
            logger.warning("Request failed (attempt %d/%d) for %s: %s. Backing off %.2fs", attempt, MAX_RETRIES, url, e, sleep_for)
            time.sleep(sleep_for)
    logger.error("All retries failed for URL: %s", url)
    return None

def parse_rss_items(xml_content: str) -> Generator[Dict, None, None]:
    """استخرج عناصر RSS بشكل آمن (title, link, pubDate, description/content)"""
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all("item")
    for it in items:
        try:
            title = it.title.text if it.title else ""
            link = it.link.text if it.link else ""
            pubDate = it.pubDate.text if it.pubDate else ""
            content = ""
            if it.find("content:encoded"):
                content = it.find("content:encoded").text
            elif it.description:
                content = it.description.text
            yield {
                "source": "snn",
                "title": title.strip(),
                "link": link.strip(),
                "pubDate": pubDate.strip(),
                "content": content.strip()
            }
        except Exception as e:
            logger.exception("Failed to parse one RSS item: %s", e)
            continue

def fingerprint_for_item(item: Dict) -> str:
    """بصمة بسيطة للعناصر (title + host)"""
    t = (item.get("title","") + "|" + urlparse(item.get("link","")).netloc).strip().lower()
    import hashlib
    return hashlib.sha256(t.encode("utf-8")).hexdigest()[:20]

def produce_to_kafka(topic: str, payload: Dict):
    """أرسل رسالة JSON إلى Kafka مع retry بسيط"""
    try:
        producer.produce(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.debug("Produced message to %s: %s", topic, payload.get("title","<no-title>")[:80])
    except BufferError as e:
        logger.error("Local producer queue is full: %s", e)
        producer.flush()
        producer.produce(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
    except Exception as e:
        logger.exception("Failed to produce message to Kafka: %s", e)

def send_heartbeat():
    """نشر heartbeat بسيط إلى Kafka ليعرف Orchestrator أن الموصل حي"""
    hb = {
        "connector_id": CONNECTOR_ID,
        "ts": datetime.now(timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "status": "running"
    }
    try:
        producer.produce(KAFKA_HEARTBEAT_TOPIC, json.dumps(hb, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.debug("Heartbeat sent")
    except Exception as e:
        logger.debug("Heartbeat publish failed: %s", e)

# ---------------------------
# Core connector logic
# ---------------------------
def enrich_item(item: Dict) -> Dict:
    """
    إثراء العنصر قبل الإرسال:
    - كشف paywall (is_paywalled)
    - جلب metrics أولية (fetch_metrics_for_url)
    - إضافة fingerprint و_meta قياسي
    - تسجيل مصدر في source registry إن أمكن
    """
    url = item.get("link", "")
    # paywall check
    try:
        paywalled, pw_details = is_paywalled(url)
    except Exception as e:
        logger.warning("Paywall check failed for %s: %s", url, e)
        paywalled, pw_details = False, {"error": "paywall_check_error"}

    # metrics
    try:
        metrics = fetch_metrics_for_url(url)
    except Exception as e:
        logger.warning("Metrics fetch failed for %s: %s", url, e)
        metrics = {"views": None, "likes": None, "shares": None, "method": "error"}

    # fingerprint and meta
    fp = fingerprint_for_item(item)
    parsed = urlparse(url)
    source_host = parsed.netloc if parsed.netloc else item.get("source", "snn")
    fetched_at = datetime.now(timezone.utc).isoformat()

    item["_meta"] = {
        "connector_id": CONNECTOR_ID,
        "fetched_at": fetched_at,
        "fetched_by": socket.gethostname(),
        "source_host": source_host,
        "fingerprint": fp,
        "paywalled": bool(paywalled),
        "paywall_details": pw_details
    }
    item["metrics"] = metrics

    # register source (best-effort)
    try:
        register_source_if_new(source_host, {"discovered_by": CONNECTOR_ID})
    except Exception:
        pass

    return item

def fetch_and_produce_rss(rss_url: str, topic: str = KAFKA_RAW_TOPIC, max_items: int = MAX_ITEMS_PER_RUN):
    """اجلب RSS وأرسل العناصر إلى Kafka بعد الإثراء والتحقق"""
    logger.info("Starting RSS fetch from %s", rss_url)

    if not is_allowed_by_robots(rss_url):
        logger.warning("robots.txt disallows scraping %s — aborting fetch", rss_url)
        return

    headers = {"User-Agent": USER_AGENT}
    resp = safe_request_get(rss_url, headers)
    if resp is None:
        logger.error("RSS fetch failed for %s", rss_url)
        return

    count = 0
    for item in parse_rss_items(resp.content):
        if count >= max_items:
            logger.info("Reached max items per run (%d). Stopping.", max_items)
            break
        try:
            enriched = enrich_item(item)
            # policy: skip paywalled items from automatic ingestion; send to review topic instead
            if enriched["_meta"].get("paywalled", False):
                review_payload = {
                    "origin_connector": CONNECTOR_ID,
                    "ingest_ts": datetime.now(timezone.utc).isoformat(),
                    "item": enriched
                }
                produce_to_kafka(f"{topic}.paywalled_review", review_payload)
                logger.info("Sent paywalled item to review topic: %s", enriched.get("link"))
                continue

            # final payload shape for downstream agents
            payload = {
                "origin_connector": CONNECTOR_ID,
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
                "item": enriched
            }
            produce_to_kafka(topic, payload)
            count += 1
        except Exception as e:
            logger.exception("Error while producing item: %s", e)
            continue

    logger.info("Completed RSS fetch run. Produced %d items.", count)

def scrape_fallback_and_produce(page_url: str, topic: str = KAFKA_RAW_TOPIC):
    """حل احتياطي: سحب الصفحة الرئيسية أو صفحة الأخبار إذا لم يتوفر RSS"""
    if not ENABLE_SCRAPE_FALLBACK:
        logger.info("Scrape fallback disabled by configuration.")
        return

    logger.info("Attempting scrape fallback for %s", page_url)
    if not is_allowed_by_robots(page_url):
        logger.warning("robots.txt disallows scraping %s — aborting scrape", page_url)
        return

    headers = {"User-Agent": USER_AGENT}
    resp = safe_request_get(page_url, headers)
    if resp is None:
        logger.error("Scrape fallback failed for %s", page_url)
        return

    soup = BeautifulSoup(resp.content, "html.parser")
    # generic extraction: articles or list items
    articles = soup.find_all(["article", "div"], limit=MAX_ITEMS_PER_RUN)
    produced = 0
    for a in articles:
        try:
            title_tag = a.find(["h1","h2","h3","a"])
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link = title_tag.get("href") or ""
            if link and link.startswith("/"):
                parsed = urlparse(page_url)
                link = f"{parsed.scheme}://{parsed.netloc}{link}"
            snippet = a.get_text(" ", strip=True)[:1200]
            item = {
                "source": "snn",
                "title": title,
                "link": link,
                "pubDate": "",
                "content": snippet
            }
            enriched = enrich_item(item)
            if enriched["_meta"].get("paywalled", False):
                produce_to_kafka(f"{topic}.paywalled_review", {"origin_connector": CONNECTOR_ID, "ingest_ts": datetime.now(timezone.utc).isoformat(), "item": enriched})
                continue
            enriched_payload = {"origin_connector": CONNECTOR_ID, "ingest_ts": datetime.now(timezone.utc).isoformat(), "item": enriched}
            produce_to_kafka(topic, enriched_payload)
            produced += 1
        except Exception as e:
            logger.exception("Error parsing article element: %s", e)
            continue

    logger.info("Scrape fallback produced %d items from %s", produced, page_url)

# ---------------------------
# Runner / Scheduler loop
# ---------------------------
def run_loop_once():
    """تشغيل دورة واحدة: fetch RSS ثم fallback"""
    fetch_and_produce_rss(SNN_RSS_URL)
    # short pause then fallback
    time.sleep(1)
    if ENABLE_SCRAPE_FALLBACK:
        # use base host as fallback page
        parsed = urlparse(SNN_RSS_URL)
        base = f"{parsed.scheme}://{parsed.netloc}"
        scrape_fallback_and_produce(base)

def run_loop_continuous():
    """حلقة رئيسية تعمل باستمرار وتبث heartbeat كل HEARTBEAT_INTERVAL"""
    logger.info("Starting snn_connector continuous run. Poll interval: %ds", POLL_INTERVAL_SECONDS)
    last_heartbeat = 0
    while True:
        try:
            run_loop_once()
            # heartbeat
            now_ts = time.time()
            if now_ts - last_heartbeat >= HEARTBEAT_INTERVAL:
                send_heartbeat()
                last_heartbeat = now_ts
            time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Connector interrupted by user. Flushing producer and exiting.")
            producer.flush()
            break
        except Exception as e:
            logger.exception("Unexpected error in run loop: %s", e)
            time.sleep(min(60, POLL_INTERVAL_SECONDS))

# ---------------------------
# CLI entrypoint
# ---------------------------
if __name__ == "__main__":
    mode = os.getenv("CONNECTOR_MODE", "loop")  # "once" or "loop"
    logger.info("snn_connector starting up. Mode=%s Kafka=%s Topic=%s", mode, KAFKA_BOOTSTRAP, KAFKA_RAW_TOPIC)
    if mode == "once":
        run_loop_once()
    else:
        run_loop_continuous()
