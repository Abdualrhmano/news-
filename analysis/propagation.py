#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis/propagation.py

وحدة قياس وتتبع سرعة انتشار الخبر (Propagation / Virality Tracker).

الهدف:
- استهلاك السجلات المجمعة من deduper (topic: tech.dedeped أو tech.deduped)
- حساب مؤشرات انتشار زمنية لكل canonical story:
    * mentions_per_hour (series)
    * propagation_speed (mentions/hour normalized)
    * growth_rate (slope over window)
    * doubling_time (تقديري)
    * peak_rate وtime_to_peak
- نشر النتائج إلى Kafka topic: tech.propagation
- تخزين time-series في Postgres (timescale) أو MinIO كنسخة احتياطية
- توفير دوال تحليلية يمكن استدعاؤها من Orchestrator أو Agent Analyzer

ملاحظات تصميمية:
- يعتمد على بيانات mentions timestamps وmetrics المجمعة في سجل canonical.
- يعمل كخدمة مستقلة أو كـconsumer ضمن Orchestrator.
- قابل للتوسع: استبدال التخزين المحلي بـTimescaleDB أو InfluxDB لاحقاً.
"""

from typing import Dict, Any, List, Optional, Tuple
import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict
import math

# Optional libs
try:
    import numpy as np
    HAS_NUMPY = True
except Exception:
    HAS_NUMPY = False

from confluent_kafka import Consumer, Producer

# Optional Postgres for time-series storage
try:
    import psycopg2
    import psycopg2.extras
    HAS_PG = True
except Exception:
    HAS_PG = False

# Optional MinIO for fallback storage
try:
    import boto3
    HAS_MINIO = True
except Exception:
    HAS_MINIO = False

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEDUPED_TOPIC = os.getenv("DEDUPED_TOPIC", "tech.deduped")
PROPAGATION_TOPIC = os.getenv("PROPAGATION_TOPIC", "tech.propagation")
GROUP_ID = os.getenv("PROPAGATION_GROUP_ID", "propagation-group")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
ENABLE_POSTGRES = os.getenv("ENABLE_POSTGRES", "false").lower() == "true" and POSTGRES_DSN != ""
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "media-intel-propagation")
ENABLE_MINIO = os.getenv("ENABLE_MINIO", "true").lower() == "true"
ANALYSIS_WINDOW_HOURS = int(os.getenv("ANALYSIS_WINDOW_HOURS", "24"))  # window to compute growth
SMOOTHING_WINDOW = int(os.getenv("SMOOTHING_WINDOW", "3"))  # smoothing for rates (hours)
LOG_LEVEL = os.getenv("PROPAGATION_LOG_LEVEL", "INFO")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [propagation] %(message)s")
logger = logging.getLogger("propagation")

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
consumer.subscribe([DEDUPED_TOPIC])

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# ---------------------------
# Postgres / MinIO clients
# ---------------------------
pg_conn = None
if ENABLE_POSTGRES and HAS_PG:
    try:
        pg_conn = psycopg2.connect(POSTGRES_DSN)
        pg_conn.autocommit = True
        logger.info("Connected to Postgres for propagation storage.")
        # ensure table exists (simple timeseries table)
        with pg_conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS propagation_metrics (
                canonical_id TEXT,
                ts TIMESTAMPTZ,
                metric JSONB,
                PRIMARY KEY (canonical_id, ts)
            );
            """)
    except Exception as e:
        logger.exception("Failed to init Postgres: %s", e)
        pg_conn = None

s3_client = None
if ENABLE_MINIO and HAS_MINIO:
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
    except Exception as e:
        logger.warning("MinIO init failed: %s", e)
        s3_client = None

# ---------------------------
# Utility helpers
# ---------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_iso(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        # fallback: try common formats
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")

def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)

# ---------------------------
# Core analytics functions
# ---------------------------
def build_mentions_histogram(mentions: List[Dict[str, Any]], window_hours: int = ANALYSIS_WINDOW_HOURS) -> Dict[str, int]:
    """
    mentions: list of mention dicts each containing 'fetched_at' or timestamp string
    returns: dict mapping hour_iso -> count
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=window_hours)
    counts = Counter()
    for m in mentions:
        ts = m.get("fetched_at") or m.get("pubDate") or m.get("first_seen")
        if not ts:
            continue
        try:
            dt = parse_iso(ts)
        except Exception:
            continue
        if dt < window_start:
            continue
        hour = floor_hour(dt)
        counts[hour.isoformat()] += 1
    # ensure continuous series for window
    series = {}
    for h in range(window_hours + 1):
        hour_dt = floor_hour(now - timedelta(hours=h))
        series[hour_dt.isoformat()] = counts.get(hour_dt.isoformat(), 0)
    # return series ordered by time ascending
    ordered = dict(sorted(series.items()))
    return ordered

def compute_rates_from_histogram(hist: Dict[str, int]) -> Dict[str, Any]:
    """
    Given hourly histogram (iso -> count), compute:
      - total_mentions
      - mentions_per_hour list
      - smoothed_rates (moving average)
      - growth_rate (slope of linear regression on log(count+1) vs time)
      - doubling_time (hours) estimated from growth_rate
      - peak_rate and time_to_peak (hours from first)
    """
    hours = list(hist.keys())
    counts = [hist[h] for h in hours]
    total = sum(counts)
    # convert to numeric x (0..n-1)
    x = list(range(len(counts)))
    # smoothing (simple moving average)
    if SMOOTHING_WINDOW > 1:
        smoothed = []
        w = SMOOTHING_WINDOW
        for i in range(len(counts)):
            window_vals = counts[max(0, i-w+1):i+1]
            smoothed.append(sum(window_vals)/len(window_vals))
    else:
        smoothed = counts[:]

    # growth rate: linear regression on log(count+1)
    try:
        if HAS_NUMPY:
            y = np.log(np.array(counts) + 1.0)
            X = np.vstack([np.array(x), np.ones(len(x))]).T
            # slope, intercept via least squares
            slope, intercept = np.linalg.lstsq(X, y, rcond=None)[0]
            growth_rate = float(slope)  # per hour in log-space
        else:
            # simple manual slope calculation
            n = len(x)
            if n >= 2:
                x_mean = sum(x)/n
                y_vals = [math.log(c+1) for c in counts]
                y_mean = sum(y_vals)/n
                num = sum((xi - x_mean)*(yi - y_mean) for xi, yi in zip(x, y_vals))
                den = sum((xi - x_mean)**2 for xi in x)
                slope = num/den if den != 0 else 0.0
                growth_rate = float(slope)
            else:
                growth_rate = 0.0
    except Exception as e:
        logger.debug("Growth rate calc failed: %s", e)
        growth_rate = 0.0

    # doubling time estimate: if growth_rate > 0, doubling_time = ln(2)/growth_rate (in hours)
    doubling_time = None
    try:
        if growth_rate > 0.0:
            doubling_time = math.log(2) / growth_rate
    except Exception:
        doubling_time = None

    # peak rate and time to peak
    peak_count = max(counts) if counts else 0
    peak_idx = counts.index(peak_count) if counts else 0
    first_idx = 0
    time_to_peak_hours = peak_idx - first_idx

    return {
        "total_mentions": total,
        "hours": hours,
        "counts": counts,
        "smoothed": smoothed,
        "growth_rate": growth_rate,
        "doubling_time_hours": doubling_time,
        "peak_count": peak_count,
        "time_to_peak_hours": time_to_peak_hours
    }

def compute_propagation_metrics(aggregated: Dict[str, Any]) -> Dict[str, Any]:
    """
    aggregated: canonical aggregated record produced by deduper, contains 'mentions' list
    returns: propagation metrics dict
    """
    mentions = aggregated.get("mentions", []) if aggregated else []
    hist = build_mentions_histogram(mentions)
    rates = compute_rates_from_histogram(hist)
    # additional normalized speed: mentions per hour normalized by sqrt(total_sources) to reduce bias
    source_count = len({m.get("source") for m in mentions if m.get("source")})
    normalized_speed = None
    try:
        normalized_speed = (rates["smoothed"][-1] if rates["smoothed"] else 0) / math.sqrt(max(1, source_count))
    except Exception:
        normalized_speed = rates["smoothed"][-1] if rates["smoothed"] else 0

    metrics = {
        "canonical_id": aggregated.get("canonical_id") or aggregated.get("canonical_link") or "unknown",
        "computed_at": now_iso(),
        "total_mentions": rates["total_mentions"],
        "mentions_per_hour": dict(zip(rates["hours"], rates["counts"])),
        "smoothed_mentions_per_hour": dict(zip(rates["hours"], [float(x) for x in rates["smoothed"]])),
        "growth_rate": rates["growth_rate"],
        "doubling_time_hours": rates["doubling_time_hours"],
        "peak_count": rates["peak_count"],
        "time_to_peak_hours": rates["time_to_peak_hours"],
        "normalized_speed": normalized_speed,
        "source_count": source_count
    }
    return metrics

# ---------------------------
# Persistence & publish
# ---------------------------
def store_metrics_postgres(canonical_id: str, ts: str, metric: Dict[str, Any]):
    if not pg_conn:
        return
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
            INSERT INTO propagation_metrics (canonical_id, ts, metric)
            VALUES (%s, %s, %s)
            ON CONFLICT (canonical_id, ts) DO UPDATE
            SET metric = EXCLUDED.metric;
            """, (canonical_id, ts, json.dumps(metric, ensure_ascii=False)))
    except Exception as e:
        logger.debug("Postgres store failed: %s", e)

def store_metrics_minio(canonical_id: str, metric: Dict[str, Any]):
    if not s3_client:
        return
    try:
        key = f"propagation/{canonical_id}/{int(time.time())}.json"
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=json.dumps(metric, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        logger.debug("MinIO store failed: %s", e)

def publish_propagation(metric: Dict[str, Any]):
    try:
        producer.produce(PROPAGATION_TOPIC, json.dumps(metric, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.info("Published propagation metrics for %s (mentions=%d)", metric.get("canonical_id"), metric.get("total_mentions", 0))
    except Exception as e:
        logger.exception("Failed to publish propagation metric: %s", e)

# ---------------------------
# Main processing for a canonical aggregated record
# ---------------------------
def process_canonical_record(record: Dict[str, Any]):
    """
    record: expected shape:
      {
        "canonical_id": "...",
        "aggregated": { ... mentions: [ {fetched_at, source, metrics, ...}, ... ] },
        "published_at": "..."
      }
    """
    canonical_id = record.get("canonical_id") or record.get("aggregated", {}).get("canonical_link") or "unknown"
    aggregated = record.get("aggregated", {})
    # attach canonical_id into aggregated for convenience
    aggregated["canonical_id"] = canonical_id
    try:
        metrics = compute_propagation_metrics(aggregated)
        ts = metrics.get("computed_at", now_iso())
        # persist
        if ENABLE_POSTGRES and pg_conn:
            store_metrics_postgres(canonical_id, ts, metrics)
        if ENABLE_MINIO and s3_client:
            store_metrics_minio(canonical_id, metrics)
        # publish to Kafka for downstream agents (Analyzer, Orchestrator)
        publish_propagation(metrics)
    except Exception as e:
        logger.exception("Failed to process canonical record %s: %s", canonical_id, e)

# ---------------------------
# Runner loop: consume deduped topic
# ---------------------------
def run_loop():
    logger.info("Propagation agent starting. Subscribed to %s", DEDUPED_TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Kafka consumer error: %s", msg.error())
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                # payload expected to have canonical_id and aggregated
                process_canonical_record(payload)
            except Exception as e:
                logger.exception("Failed to handle message: %s", e)
    except KeyboardInterrupt:
        logger.info("Propagation agent interrupted. Exiting.")
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
