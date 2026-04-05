#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics/metrics_collector.py
وحدة احترافية لجمع مقاييس التفاعل للمقالات (views, likes, shares, comments, social signals).
الهدف: توفير دالة موحدة fetch_metrics_for_url(url) تُعيد dict بالقياسات وتخزن time-series في Postgres/TimescaleDB
أو في ملف JSON على MinIO إذا لم يتوفر DB. كما تنشر تحديثات إلى Kafka topic (tech.metrics) ليستهلكها باقي الوكلاء.
مزايا:
- دعم RSS/meta scraping وOpenGraph وJSON-LD لاستخراج مؤشرات التفاعل إن وُجدت.
- محاولات retry وexponential backoff، واحترام robots.txt.
- واجهة قابلة للاستخدام من الموصلات: enrich_item_with_metrics_and_paywall يستدعي fetch_metrics_for_url.
- تخزين محلي احتياطي في MinIO (S3 compatible) عند عدم توفر DB.
- نشر تحديثات إلى Kafka topic لتغذية Agent Analyzer وAgent Propagation.
ملاحظة: هذا ملف يعتمد على حزم موجودة في requirements.txt (requests, beautifulsoup4, confluent-kafka, boto3, psycopg2-binary أو sqlalchemy).
"""

import os
import time
import json
import logging
import random
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from confluent_kafka import Producer
import boto3

# Optional DB imports (use if available)
try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PG = True
except Exception:
    HAS_PG = False

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_METRICS_TOPIC = os.getenv("KAFKA_METRICS_TOPIC", "tech.metrics")
USER_AGENT = os.getenv("METRICS_USER_AGENT", "MediaIntelMetrics/1.0 (+https://example.com)")
REQUEST_TIMEOUT = int(os.getenv("METRICS_REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("METRICS_MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("METRICS_BACKOFF_BASE", "1.5"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "media-intel-metrics")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")  # e.g., "postgresql://user:pass@localhost:5432/dbname"
ENABLE_MINIO = os.getenv("ENABLE_MINIO", "true").lower() == "true"
ENABLE_POSTGRES = os.getenv("ENABLE_POSTGRES", "false").lower() == "true" and POSTGRES_DSN != ""
ROBOTS_USER_AGENT = os.getenv("ROBOTS_USER_AGENT", USER_AGENT)
LOG_LEVEL = os.getenv("METRICS_LOG_LEVEL", "INFO")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [metrics_collector] %(message)s")
logger = logging.getLogger("metrics_collector")

# ---------------------------
# Kafka producer
# ---------------------------
producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
producer = Producer(producer_conf)

# ---------------------------
# MinIO client (S3 compatible) for fallback storage
# ---------------------------
s3_client = None
if ENABLE_MINIO:
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        # ensure bucket exists (best-effort)
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            s3_client.create_bucket(Bucket=MINIO_BUCKET)
    except Exception as e:
        logger.warning("MinIO client init failed: %s", e)
        s3_client = None

# ---------------------------
# Postgres connection helper (optional)
# ---------------------------
pg_conn = None
if ENABLE_POSTGRES and HAS_PG:
    try:
        pg_conn = psycopg2.connect(POSTGRES_DSN)
        pg_conn.autocommit = True
        logger.info("Connected to Postgres for metrics storage.")
    except Exception as e:
        logger.warning("Failed to connect to Postgres: %s", e)
        pg_conn = None

# ---------------------------
# Helpers: robots, requests, parsing
# ---------------------------
def is_allowed_by_robots(url: str, user_agent: str = ROBOTS_USER_AGENT) -> bool:
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
        logger.debug("robots check failed for %s: %s", url, e)
        return True

def safe_get(url: str, headers: dict = None, timeout: int = REQUEST_TIMEOUT) -> Optional[requests.Response]:
    headers = headers or {"User-Agent": USER_AGENT}
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            attempt += 1
            sleep_for = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.5)
            logger.debug("GET attempt %d failed for %s: %s. Backoff %.2fs", attempt, url, e, sleep_for)
            time.sleep(sleep_for)
    logger.error("All GET retries failed for %s", url)
    return None

def parse_open_graph(soup: BeautifulSoup) -> Dict:
    og = {}
    for tag in soup.find_all("meta"):
        if tag.get("property", "").startswith("og:") or tag.get("name", "").startswith("og:"):
            key = tag.get("property") or tag.get("name")
            og[key] = tag.get("content", "")
    return og

def parse_json_ld(soup: BeautifulSoup) -> Dict:
    """حاول استخراج JSON-LD (schema.org) إن وُجد"""
    data = {}
    try:
        scripts = soup.find_all("script", type="application/ld+json")
        for s in scripts:
            try:
                j = json.loads(s.string)
                # قد يكون قائمة أو كائن
                if isinstance(j, list):
                    for item in j:
                        data.update(item if isinstance(item, dict) else {})
                elif isinstance(j, dict):
                    data.update(j)
            except Exception:
                continue
    except Exception:
        pass
    return data

# ---------------------------
# Core metric extraction logic
# ---------------------------
def extract_metrics_from_html(html: str, url: str) -> Dict:
    """
    يحاول استخراج مؤشرات التفاعل من HTML:
    - قد نجد عناصر مثل <meta property="article:tag"> أو عناصر تعرض عدد المشاهدات أو الإعجابات.
    - يستخرج OpenGraph وJSON-LD كمصادر أولية.
    - يعيد dict يحتوي على keys: views, likes, shares, comments, extracted (raw fields)
    """
    soup = BeautifulSoup(html, "html.parser")
    og = parse_open_graph(soup)
    jsonld = parse_json_ld(soup)

    metrics = {"views": None, "likes": None, "shares": None, "comments": None, "extracted": {}}

    # 1) JSON-LD common fields
    try:
        if jsonld:
            # بعض المواقع تضع interactionStatistic في JSON-LD
            if isinstance(jsonld, dict):
                if "interactionStatistic" in jsonld:
                    stats = jsonld.get("interactionStatistic")
                    # قد يكون قائمة
                    if isinstance(stats, list):
                        for s in stats:
                            typ = s.get("interactionType", {}).get("name", "").lower()
                            val = s.get("userInteractionCount")
                            if "view" in typ and val:
                                metrics["views"] = int(val)
                            if "like" in typ and val:
                                metrics["likes"] = int(val)
                    elif isinstance(stats, dict):
                        typ = stats.get("interactionType", {}).get("name", "").lower()
                        val = stats.get("userInteractionCount")
                        if "view" in typ and val:
                            metrics["views"] = int(val)
    except Exception:
        logger.debug("JSON-LD parsing for metrics failed for %s", url)

    # 2) OpenGraph / meta tags (some sites expose share counts)
    try:
        # بعض المواقع تضع og:views أو custom meta tags
        for k, v in og.items():
            lk = k.lower()
            if "views" in lk and v and metrics["views"] is None:
                try:
                    metrics["views"] = int(v)
                except Exception:
                    pass
            if "likes" in lk and v and metrics["likes"] is None:
                try:
                    metrics["likes"] = int(v)
                except Exception:
                    pass
    except Exception:
        pass

    # 3) heuristic: search for common CSS classes that show counts
    try:
        # أمثلة: .views-count, .article-views, .share-count, .likes-count
        selectors = [".views-count", ".article-views", ".share-count", ".likes-count", ".post-views"]
        for sel in selectors:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                txt = el.get_text(strip=True).replace(",", "")
                try:
                    val = int("".join([c for c in txt if c.isdigit()]))
                    if "view" in sel and metrics["views"] is None:
                        metrics["views"] = val
                    elif "like" in sel and metrics["likes"] is None:
                        metrics["likes"] = val
                    elif "share" in sel and metrics["shares"] is None:
                        metrics["shares"] = val
                except Exception:
                    continue
    except Exception:
        pass

    # 4) fallback: look for textual patterns like "X views" or "X مشاهدة"
    try:
        text = soup.get_text(" ", strip=True)[:5000].lower()
        import re
        m = re.search(r"([\d,\.]+)\s+(views|view|مشاهدة|مشاهدات)", text)
        if m and metrics["views"] is None:
            num = m.group(1).replace(",", "").replace(".", "")
            try:
                metrics["views"] = int(num)
            except Exception:
                pass
    except Exception:
        pass

    metrics["extracted"].update({"og": og, "jsonld_keys": list(jsonld.keys()) if isinstance(jsonld, dict) else []})
    return metrics

# ---------------------------
# Public API: fetch_metrics_for_url
# ---------------------------
def fetch_metrics_for_url(url: str, allow_scrape: bool = True) -> Dict:
    """
    الدالة الأساسية التي يستدعيها الموصل أو أي وكيل آخر.
    تعيد dict بالشكل:
      {
        "url": url,
        "fetched_at": iso_ts,
        "views": int|None,
        "likes": int|None,
        "shares": int|None,
        "comments": int|None,
        "source": host,
        "method": "jsonld|og|scrape|none",
        "raw": {...},  # مقتطفات مساعدة
        "error": None|str
      }
    كما تقوم بتخزين السجل في Postgres أو MinIO، وتنشر رسالة إلى Kafka topic tech.metrics.
    """
    result = {
        "url": url,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "views": None,
        "likes": None,
        "shares": None,
        "comments": None,
        "source": urlparse(url).netloc,
        "method": None,
        "raw": {},
        "error": None
    }

    # robots check
    try:
        if not is_allowed_by_robots(url):
            result["error"] = "disallowed_by_robots"
            logger.info("Robots disallow fetching metrics for %s", url)
            return result
    except Exception as e:
        logger.debug("robots check error: %s", e)

    resp = safe_get(url)
    if resp is None:
        result["error"] = "fetch_failed"
        return result

    try:
        metrics = extract_metrics_from_html(resp.text, url)
        result.update({
            "views": metrics.get("views"),
            "likes": metrics.get("likes"),
            "shares": metrics.get("shares"),
            "comments": metrics.get("comments"),
            "raw": metrics.get("extracted", {}),
            "method": "html_extraction"
        })
    except Exception as e:
        logger.exception("Failed to extract metrics for %s: %s", url, e)
        result["error"] = "extract_failed"
        result["raw"] = {}

    # store to Postgres if available
    try:
        if pg_conn:
            cur = pg_conn.cursor()
            # simple table schema assumed: metrics(url text, fetched_at timestamptz, views int, likes int, shares int, comments int, raw jsonb)
            insert_sql = """
            INSERT INTO metrics (url, fetched_at, views, likes, shares, comments, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_sql, (
                result["url"],
                result["fetched_at"],
                result["views"],
                result["likes"],
                result["shares"],
                result["comments"],
                json.dumps(result["raw"], ensure_ascii=False)
            ))
            cur.close()
    except Exception as e:
        logger.debug("Postgres insert failed: %s", e)

    # fallback store to MinIO if configured
    try:
        if s3_client:
            key = f"metrics/{urlparse(url).netloc}/{int(time.time())}.json"
            s3_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=json.dumps(result, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        logger.debug("MinIO store failed: %s", e)

    # publish to Kafka topic for downstream agents
    try:
        producer.produce(KAFKA_METRICS_TOPIC, json.dumps(result, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
    except Exception as e:
        logger.exception("Failed to publish metrics to Kafka: %s", e)

    return result

# ---------------------------
# CLI quick test
# ---------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch metrics for a URL")
    parser.add_argument("url", help="URL to fetch metrics for")
    args = parser.parse_args()
    res = fetch_metrics_for_url(args.url)
    print(json.dumps(res, indent=2, ensure_ascii=False))
