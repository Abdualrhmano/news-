#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
utils/deduper.py
وحدة احترافية لإلغاء التكرار (deduplication) وتجميع mentions لنفس الخبر من مصادر متعددة.
الهدف:
- اكتشاف نسخ نفس الخبر (same story) عبر عناوين وروابط ومحتوى مشابه
- إنشاء canonical_id لكل قصة وتجميع mentions (sources, timestamps, metrics)
- نشر السجل المجمّع إلى Kafka topic (tech.deduped أو tech.signals) ليستهلكه Agent Analyzer
- تخزين mapping وhistory في Postgres أو Redis ونسخ raw aggregation إلى MinIO
مزايا:
- fingerprinting متعدد الأساليب (title fingerprint, content hash, fuzzy matching)
- تجميع تدريجي: كل رسالة raw تُمرّر إلى الديدوبر وتُحدّث السجل القنوني
- سياسات TTL وmerge thresholds قابلة للتعديل عبر env vars
- logging مفصّل وmetrics للنظام
- قابل للتشغيل كخدمة أو كـconsumer مستقل يقرأ من Kafka topic tech.raw.*
"""

import os
import time
import json
import logging
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone

# نصائح: rapidfuzz أسرع من difflib للمقارنات الضبابية، لكن قد لا يكون مثبتاً افتراضياً.
try:
    from rapidfuzz import fuzz
    FUZZ_AVAILABLE = True
except Exception:
    import difflib
    FUZZ_AVAILABLE = False

from confluent_kafka import Consumer, Producer
from urllib.parse import urlparse
import psycopg2
import psycopg2.extras
import boto3

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPICS = os.getenv("RAW_TOPICS", "tech.raw").split(",")  # comma-separated
DEDUPED_TOPIC = os.getenv("DEDUPED_TOPIC", "tech.deduped")
GROUP_ID = os.getenv("DEDUPER_GROUP_ID", "deduper-group")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")  # e.g., postgresql://user:pass@host:5432/db
ENABLE_POSTGRES = os.getenv("ENABLE_POSTGRES", "false").lower() == "true" and POSTGRES_DSN != ""
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "media-intel-dedup")
ENABLE_MINIO = os.getenv("ENABLE_MINIO", "true").lower() == "true"
FUZZ_THRESHOLD = float(os.getenv("FUZZ_THRESHOLD", "85.0"))  # threshold for fuzzy title match (0-100)
CONTENT_HASH_SIMILARITY = float(os.getenv("CONTENT_HASH_SIMILARITY", "0.9"))  # not used directly, placeholder
CANONICAL_TTL_SECONDS = int(os.getenv("CANONICAL_TTL_SECONDS", str(60*60*24*7)))  # keep canonical records 7 days by default
LOG_LEVEL = os.getenv("DEDUPER_LOG_LEVEL", "INFO")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [deduper] %(message)s")
logger = logging.getLogger("deduper")

# ---------------------------
# Kafka consumer/producer
# ---------------------------
consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}
consumer = Consumer(consumer_conf)
consumer.subscribe(RAW_TOPICS)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# ---------------------------
# Postgres (canonical store) - optional but recommended
# ---------------------------
pg_conn = None
if ENABLE_POSTGRES:
    try:
        pg_conn = psycopg2.connect(POSTGRES_DSN)
        pg_conn.autocommit = True
        logger.info("Connected to Postgres for deduper canonical store.")
        # ensure table exists (simple schema)
        with pg_conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS canonical_stories (
                canonical_id TEXT PRIMARY KEY,
                title TEXT,
                canonical_link TEXT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                aggregated JSONB
            );
            """)
    except Exception as e:
        logger.exception("Failed to init Postgres connection: %s", e)
        pg_conn = None

# ---------------------------
# MinIO client for storing aggregated raw bundles
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
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logger.info("MinIO client initialized for deduper.")
    except Exception as e:
        logger.warning("MinIO init failed: %s", e)
        s3_client = None

# ---------------------------
# Utility helpers
# ---------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def title_fingerprint(title: str) -> str:
    """بسيط: normalize + hash"""
    t = title.strip().lower()
    # remove punctuation and extra spaces
    t = "".join(ch for ch in t if ch.isalnum() or ch.isspace())
    t = " ".join(t.split())
    return sha256_text(t)[:16]

def content_hash(content: str) -> str:
    """hash للمحتوى الكامل (يمكن استخدام shingling لاحقاً)"""
    return sha256_text(content.strip().lower())

def fuzzy_title_similarity(a: str, b: str) -> float:
    """إرجاع نسبة تشابه 0..100"""
    if FUZZ_AVAILABLE:
        try:
            return fuzz.token_sort_ratio(a, b)
        except Exception:
            return 0.0
    else:
        # fallback to difflib ratio (0..1) convert to 0..100
        try:
            return difflib.SequenceMatcher(None, a, b).ratio() * 100.0
        except Exception:
            return 0.0

def generate_canonical_id(title: str, link: str) -> str:
    base = f"{title}||{link}"
    return "canon_" + sha256_text(base)[:20]

# ---------------------------
# Canonical store operations
# ---------------------------
def get_canonical_by_id(canonical_id: str) -> Optional[Dict[str, Any]]:
    if not pg_conn:
        return None
    try:
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM canonical_stories WHERE canonical_id = %s", (canonical_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.debug("Postgres get error: %s", e)
        return None

def upsert_canonical(canonical_id: str, title: str, canonical_link: str, aggregated: Dict[str, Any]):
    if not pg_conn:
        return
    try:
        now = datetime.now(timezone.utc)
        with pg_conn.cursor() as cur:
            cur.execute("""
            INSERT INTO canonical_stories (canonical_id, title, canonical_link, created_at, updated_at, aggregated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (canonical_id) DO UPDATE
            SET title = EXCLUDED.title,
                canonical_link = EXCLUDED.canonical_link,
                updated_at = EXCLUDED.updated_at,
                aggregated = EXCLUDED.aggregated;
            """, (canonical_id, title, canonical_link, now, now, json.dumps(aggregated, ensure_ascii=False)))
    except Exception as e:
        logger.exception("Failed to upsert canonical: %s", e)

# ---------------------------
# Aggregation / merge logic
# ---------------------------
def merge_mention_into_canonical(canonical: Dict[str, Any], mention: Dict[str, Any]) -> Dict[str, Any]:
    """
    canonical: existing aggregated record
    mention: raw item message
    الهدف: تحديث aggregated mentions list, update metrics aggregation, update timestamps
    """
    agg = canonical.get("aggregated", {}) if canonical else {}
    mentions = agg.get("mentions", [])
    # append mention summary
    mention_summary = {
        "id": mention.get("id") or sha256_text(mention.get("link", "") + mention.get("title", ""))[:16],
        "source": mention.get("source"),
        "link": mention.get("link"),
        "title": mention.get("title"),
        "pubDate": mention.get("pubDate"),
        "fetched_at": mention.get("_meta", {}).get("fetched_at", now_iso()),
        "metrics": mention.get("metrics", {}),
        "meta": mention.get("_meta", {})
    }
    mentions.append(mention_summary)
    # update aggregated metrics: sum views if available, keep max likes, count mentions
    total_views = agg.get("total_views", 0) or 0
    if mention_summary["metrics"].get("views"):
        try:
            total_views += int(mention_summary["metrics"]["views"])
        except Exception:
            pass
    max_likes = agg.get("max_likes", 0) or 0
    if mention_summary["metrics"].get("likes"):
        try:
            max_likes = max(max_likes, int(mention_summary["metrics"]["likes"]))
        except Exception:
            pass
    first_seen = agg.get("first_seen") or mention_summary["fetched_at"]
    last_seen = mention_summary["fetched_at"]
    # canonical title/link heuristics: keep earliest non-empty canonical_link
    canonical_link = agg.get("canonical_link") or mention_summary["link"]
    canonical_title = agg.get("title") or mention_summary["title"]

    new_agg = {
        "title": canonical_title,
        "canonical_link": canonical_link,
        "mentions": mentions,
        "mention_count": len(mentions),
        "total_views": total_views,
        "max_likes": max_likes,
        "first_seen": first_seen,
        "last_seen": last_seen
    }
    return new_agg

# ---------------------------
# Matching logic: find existing canonical candidate
# ---------------------------
def find_matching_canonical(title: str, link: str, content: Optional[str] = None) -> Optional[str]:
    """
    Strategy:
    1. If Postgres enabled: scan recent canonical_stories (last N days) and compute fuzzy title similarity.
    2. If a candidate exceeds FUZZ_THRESHOLD, return its canonical_id.
    3. Else return None (create new canonical).
    Note: For large scale, replace with inverted index or vector search.
    """
    if not pg_conn:
        return None
    try:
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # limit to recent records to keep performance
            cur.execute("SELECT canonical_id, title FROM canonical_stories WHERE updated_at >= NOW() - INTERVAL '14 days'")
            rows = cur.fetchall()
            best_id = None
            best_score = 0.0
            for r in rows:
                existing_title = r.get("title") or ""
                score = fuzzy_title_similarity(title, existing_title)
                if score > best_score:
                    best_score = score
                    best_id = r.get("canonical_id")
            logger.debug("Best fuzzy match score for title '%s' -> %s (score %.2f)", title, best_id, best_score)
            if best_score >= FUZZ_THRESHOLD:
                return best_id
            return None
    except Exception as e:
        logger.debug("Error during canonical search: %s", e)
        return None

# ---------------------------
# Publish aggregated canonical to Kafka
# ---------------------------
def publish_canonical(canonical_id: str, aggregated: Dict[str, Any]):
    payload = {
        "canonical_id": canonical_id,
        "aggregated": aggregated,
        "published_at": now_iso()
    }
    try:
        producer.produce(DEDUPED_TOPIC, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.info("Published canonical %s to %s (mentions=%d)", canonical_id, DEDUPED_TOPIC, aggregated.get("mention_count", 0))
    except Exception as e:
        logger.exception("Failed to publish canonical to Kafka: %s", e)

# ---------------------------
# Store aggregated bundle to MinIO (optional)
# ---------------------------
def store_aggregated_to_minio(canonical_id: str, aggregated: Dict[str, Any]):
    if not s3_client:
        return
    try:
        key = f"deduped/{canonical_id}/{int(time.time())}.json"
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=json.dumps(aggregated, ensure_ascii=False).encode("utf-8"))
        logger.debug("Stored aggregated bundle to MinIO: %s", key)
    except Exception as e:
        logger.debug("MinIO store failed: %s", e)

# ---------------------------
# Main processing for a single raw message
# ---------------------------
def process_raw_message(raw: Dict[str, Any]):
    """
    1. compute fingerprints/hashes
    2. try to find existing canonical via fingerprint or fuzzy title match
    3. if found: merge mention and upsert canonical
    4. if not found: create new canonical record
    5. publish aggregated canonical to Kafka
    """
    title = raw.get("title", "") or ""
    link = raw.get("link", "") or ""
    content = raw.get("content", "") or ""
    fingerprint = raw.get("_meta", {}).get("fingerprint") or f"{title}_{urlparse(link).netloc}"
    title_fp = title_fingerprint(title)
    content_h = content_hash(content) if content else None

    # 1) try exact fingerprint match in Postgres aggregated mentions (simple query)
    canonical_id = None
    if pg_conn:
        try:
            with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT canonical_id, aggregated FROM canonical_stories WHERE aggregated->'mentions' @> %s::jsonb LIMIT 1", (json.dumps([{"meta": {"fingerprint": fingerprint}}]),))
                row = cur.fetchone()
                if row:
                    canonical_id = row.get("canonical_id")
        except Exception:
            # above query is heuristic and may not work for all schemas; ignore errors
            canonical_id = None

    # 2) fuzzy title match
    if not canonical_id:
        candidate = find_matching_canonical(title, link, content)
        if candidate:
            canonical_id = candidate

    # 3) if still not found, create new canonical_id
    if not canonical_id:
        canonical_id = generate_canonical_id(title, link)
        canonical = None
        aggregated = {
            "title": title,
            "canonical_link": link,
            "mentions": [],
            "mention_count": 0,
            "total_views": 0,
            "max_likes": 0,
            "first_seen": now_iso(),
            "last_seen": now_iso()
        }
    else:
        canonical = get_canonical_by_id(canonical_id)
        aggregated = canonical.get("aggregated", {}) if canonical else {
            "title": title,
            "canonical_link": link,
            "mentions": [],
            "mention_count": 0,
            "total_views": 0,
            "max_likes": 0,
            "first_seen": now_iso(),
            "last_seen": now_iso()
        }

    # merge mention
    new_aggregated = merge_mention_into_canonical({"aggregated": aggregated}, raw)

    # upsert canonical store
    upsert_canonical(canonical_id, new_aggregated.get("title", title), new_aggregated.get("canonical_link", link), new_aggregated)

    # store aggregated snapshot to MinIO for archival
    store_aggregated_to_minio(canonical_id, new_aggregated)

    # publish to Kafka for downstream processing (Analyzer)
    publish_canonical(canonical_id, new_aggregated)

# ---------------------------
# Runner loop: consume raw topics and process
# ---------------------------
def run_loop():
    logger.info("Deduper starting. Subscribed to topics: %s", RAW_TOPICS)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Kafka consumer error: %s", msg.error())
                continue
            try:
                raw = json.loads(msg.value().decode("utf-8"))
                process_raw_message(raw)
            except Exception as e:
                logger.exception("Failed to process raw message: %s", e)
    except KeyboardInterrupt:
        logger.info("Deduper interrupted by user. Exiting.")
    finally:
        try:
            consumer.close()
        except Exception:
            pass

# ---------------------------
# CLI quick test
# ---------------------------
if __name__ == "__main__":
    run_loop()
