#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
connectors/makroohsoft_connector.py
موصل احترافي لموقع "مكروه سوفت" (RSS أو صفحة مقالات) يدفع العناصر إلى Kafka topic
ويُجري فحوصات أولية: كشف paywall، إثراء metadata، استدعاء Metrics Collector لجلب المشاهدات/التفاعلات،
وإرسال الرسالة مع حقول جاهزة للوكلاء اللاحقين (deduper, credibility, analyzer).
مزايا:
- قابل للتكوين عبر متغيرات بيئة
- احترام robots.txt وRate Limiting
- retry مع exponential backoff
- استدعاء وحدات مساعدة خارجية (paywall_detector, metrics_collector) إن وُجدت
- إنتاج رسائل JSON إلى Kafka مع metadata مفصّل
- logging وstructured events لتسهيل المراقبة
"""

import os
import time
import json
import logging
import random
import socket
from typing import Generator, Dict, Optional
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
from datetime import datetime, timezone
from urllib.robotparser import RobotFileParser

# Attempt to import optional helpers (will be used if available in repo)
try:
    from utils.paywall_detector import is_paywalled
except Exception:
    # fallback stub if module not yet present
    def is_paywalled(url: str) -> bool:
        return False

try:
    from metrics.metrics_collector import fetch_metrics_for_url
except Exception:
    def fetch_metrics_for_url(url: str) -> Dict:
        return {"views": None, "likes": None, "shares": None}

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tech.raw.makrooh")
MAK_RSS_URL = os.getenv("MAK_RSS_URL", "https://makroohsoft.example/rss")
MAK_BASE_PAGE = os.getenv("MAK_BASE_PAGE", "https://makroohsoft.example")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
USER_AGENT = os.getenv("CONNECTOR_USER_AGENT", "MediaIntelBot/1.0 (+https://example.com)")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.5"))
MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "200"))
ENABLE_SCRAPE_FALLBACK = os.getenv("ENABLE_SCRAPE_FALLBACK", "true").lower() == "true"

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(
    level=os.getenv("CONNECTOR_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [makrooh_connector] %(message)s"
)
logger = logging.getLogger("makrooh_connector")

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
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            attempt += 1
            sleep_for = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.5)
            logger.warning("Request failed (attempt %d/%d) for %s: %s. Backoff %.2fs", attempt, MAX_RETRIES, url, e, sleep_for)
            time.sleep(sleep_for)
    logger.error("All retries failed for URL: %s", url)
    return None

def parse_rss_items(xml_content: str) -> Generator[Dict, None, None]:
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
                "source": "makroohsoft",
                "title": title.strip(),
                "link": link.strip(),
                "pubDate": pubDate.strip(),
                "content": content.strip(),
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.exception("Failed to parse one RSS item: %s", e)
            continue

def produce_to_kafka(topic: str, payload: Dict):
    try:
        producer.produce(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.debug("Produced to %s: %s", topic, payload.get("title","<no-title>")[:80])
    except BufferError as e:
        logger.error("Producer queue full: %s", e)
        producer.flush()
        producer.produce(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
    except Exception as e:
        logger.exception("Failed to produce message to Kafka: %s", e)

# ---------------------------
# Core connector logic
# ---------------------------
def enrich_item_with_metrics_and_paywall(item: Dict) -> Dict:
    """
    يستدعي وحدة كشف paywall ووحدة جمع المقاييس (إن وُجدتا).
    يضيف الحقول التالية إلى الرسالة:
      - _meta.paywalled (bool)
      - metrics: {views, likes, shares}
      - _meta.source_credibility_hint (placeholder)
    """
    url = item.get("link", "")
    try:
        paywalled = is_paywalled(url)
    except Exception as e:
        logger.warning("Paywall detector failed for %s: %s", url, e)
        paywalled = False

    try:
        metrics = fetch_metrics_for_url(url)
    except Exception as e:
        logger.warning("Metrics collector failed for %s: %s", url, e)
        metrics = {"views": None, "likes": None, "shares": None}

    # enrich metadata
    item["_meta"] = item.get("_meta", {})
    item["_meta"].update({
        "fetched_by": socket.gethostname(),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "paywalled": bool(paywalled),
        "source_host": urlparse(url).netloc if url else "makroohsoft"
    })
    item["metrics"] = metrics
    # placeholder for credibility hint (will be computed by credibility agent)
    item["_meta"]["credibility_hint"] = None
    return item

def fetch_and_produce_rss(rss_url: str, topic: str = KAFKA_TOPIC, max_items: int = MAX_ITEMS_PER_RUN):
    logger.info("Starting RSS fetch from %s", rss_url)
    if not is_allowed_by_robots(rss_url):
        logger.warning("robots.txt disallows %s — aborting", rss_url)
        return

    headers = {"User-Agent": USER_AGENT}
    resp = safe_request_get(rss_url, headers)
    if resp is None:
        logger.error("RSS fetch failed for %s", rss_url)
        return

    count = 0
    for item in parse_rss_items(resp.content):
        if count >= max_items:
            logger.info("Reached max items per run (%d).", max_items)
            break
        try:
            item = enrich_item_with_metrics_and_paywall(item)
            # skip paywalled content automatically (policy: ignore paywalled sources)
            if item["_meta"].get("paywalled", False):
                logger.info("Skipping paywalled item: %s", item.get("link"))
                continue
            # add dedupe hint: simple fingerprint (title+host)
            fingerprint = f"{item.get('title','')}_{item['_meta'].get('source_host','')}"
            item["_meta"]["fingerprint"] = fingerprint
            produce_to_kafka(topic, item)
            count += 1
        except Exception as e:
            logger.exception("Error producing item: %s", e)
            continue

    logger.info("Completed RSS fetch run. Produced %d items.", count)

def scrape_fallback_and_produce(page_url: str, topic: str = KAFKA_TOPIC):
    if not ENABLE_SCRAPE_FALLBACK:
        logger.info("Scrape fallback disabled.")
        return

    logger.info("Attempting scrape fallback for %s", page_url)
    if not is_allowed_by_robots(page_url):
        logger.warning("robots.txt disallows scraping %s — aborting", page_url)
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
                "source": "makroohsoft",
                "title": title,
                "link": link,
                "pubDate": "",
                "content": snippet,
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
            item = enrich_item_with_metrics_and_paywall(item)
            if item["_meta"].get("paywalled", False):
                logger.info("Skipping paywalled scraped item: %s", item.get("link"))
                continue
            item["_meta"]["fingerprint"] = f"{item.get('title','')}_{item['_meta'].get('source_host','')}"
            produce_to_kafka(topic, item)
            produced += 1
        except Exception as e:
            logger.exception("Error parsing article element: %s", e)
            continue

    logger.info("Scrape fallback produced %d items from %s", produced, page_url)

# ---------------------------
# Runner / Scheduler loop
# ---------------------------
def run_loop():
    logger.info("makroohsoft_connector starting. Poll interval: %ds", POLL_INTERVAL_SECONDS)
    while True:
        try:
            fetch_and_produce_rss(MAK_RSS_URL)
            # fallback to base page if RSS yields nothing
            time.sleep(2)
            # optional: check if produced items recently; if none, try fallback
            scrape_fallback_and_produce(MAK_BASE_PAGE)
            time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Connector interrupted. Flushing producer and exiting.")
            producer.flush()
            break
        except Exception as e:
            logger.exception("Unexpected error in run loop: %s", e)
            time.sleep(min(60, POLL_INTERVAL_SECONDS))

# ---------------------------
# CLI entrypoint
# ---------------------------
if __name__ == "__main__":
    logger.info("makroohsoft_connector starting up. Kafka: %s Topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    run_loop()
