#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis/credibility.py

وحدة حساب مصداقية المصادر (Source Credibility Scorer).

الهدف:
- حساب درجة مصداقية لكل مصدر/دومين بناءً على إشارات متعددة:
  * عمر الدومين (WHOIS)
  * وجود/صلاحية شهادة SSL/TLS
  * وجود سياسات واضحة (about, contact, editorial)
  * سجل النشر (عدد المقالات، نسبة paywalled)
  * إشارات خارجية (blacklists, known reputable lists) — يمكن تحميلها من ملف خارجي
  * نتائج سابقة (history accuracy) إن توفرت
- إخراج score رقمي 0..1 مع تفسير (explainability) للحقل
- نشر النتيجة إلى Kafka topic `tech.credibility`
- تخزين السجل في Postgres/MinIO للرجوع والتحليل
- واجهة برمجية بسيطة (دالة compute_credibility_for_source) قابلة للاستدعاء من Orchestrator أو Agents

ملاحظات:
- بعض العمليات (WHOIS) قد تكون بطيئة أو تتطلب مكتبات خارجية؛ الكود يتضمن fallbacks.
- لا يعتمد القرار على مصدر واحد فقط؛ يستخدم مزيج إشارات مع أوزان قابلة للتعديل عبر env vars.
"""

from typing import Dict, Any, Optional, Tuple, List
import os
import time
import json
import logging
import socket
import requests
import ssl
import datetime
from urllib.parse import urlparse

# Optional libs
try:
    import whois
    HAS_WHOIS = True
except Exception:
    HAS_WHOIS = False

try:
    import tldextract
    HAS_TLDEX = True
except Exception:
    HAS_TLDEX = False

try:
    import psycopg2
    import psycopg2.extras
    HAS_PG = True
except Exception:
    HAS_PG = False

try:
    import boto3
    HAS_MINIO = True
except Exception:
    HAS_MINIO = False

from confluent_kafka import Producer

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_CRED_TOPIC = os.getenv("KAFKA_CRED_TOPIC", "tech.credibility")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
ENABLE_POSTGRES = os.getenv("ENABLE_POSTGRES", "false").lower() == "true" and POSTGRES_DSN != ""
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "media-intel-cred")
ENABLE_MINIO = os.getenv("ENABLE_MINIO", "true").lower() == "true"
LOG_LEVEL = os.getenv("CRED_LOG_LEVEL", "INFO")

# Weights for scoring (tune these)
WEIGHT_DOMAIN_AGE = float(os.getenv("W_DOMAIN_AGE", "0.25"))
WEIGHT_SSL = float(os.getenv("W_SSL", "0.15"))
WEIGHT_CONTACT = float(os.getenv("W_CONTACT", "0.10"))
WEIGHT_EDITORIAL = float(os.getenv("W_EDITORIAL", "0.10"))
WEIGHT_HISTORY = float(os.getenv("W_HISTORY", "0.20"))
WEIGHT_EXTERNAL = float(os.getenv("W_EXTERNAL", "0.20"))

# External lists file (optional) - JSON mapping domain -> reputation_score (0..1)
EXTERNAL_LISTS_PATH = os.getenv("EXTERNAL_LISTS_PATH", "data/external_reputation.json")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [credibility] %(message)s")
logger = logging.getLogger("credibility")

# ---------------------------
# Kafka producer
# ---------------------------
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# ---------------------------
# Postgres client (optional)
# ---------------------------
pg_conn = None
if ENABLE_POSTGRES and HAS_PG:
    try:
        pg_conn = psycopg2.connect(POSTGRES_DSN)
        pg_conn.autocommit = True
        logger.info("Connected to Postgres for credibility storage.")
        with pg_conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS source_credibility (
                domain TEXT PRIMARY KEY,
                computed_at TIMESTAMPTZ,
                score REAL,
                details JSONB
            );
            """)
    except Exception as e:
        logger.warning("Postgres init failed: %s", e)
        pg_conn = None

# ---------------------------
# MinIO client (optional)
# ---------------------------
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
# Load external reputation lists (optional)
# ---------------------------
_external_reputation = {}
if os.path.exists(EXTERNAL_LISTS_PATH):
    try:
        with open(EXTERNAL_LISTS_PATH, "r", encoding="utf-8") as f:
            _external_reputation = json.load(f)
            logger.info("Loaded external reputation list with %d entries", len(_external_reputation))
    except Exception as e:
        logger.warning("Failed to load external reputation lists: %s", e)
        _external_reputation = {}

# ---------------------------
# Helpers
# ---------------------------
def domain_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        # strip port
        if ":" in host:
            host = host.split(":")[0]
        return host
    except Exception:
        return url

def get_domain_age_years(domain: str) -> Optional[float]:
    """حاول استخدام whois للحصول على عمر الدومين بالسنوات"""
    if not HAS_WHOIS:
        logger.debug("whois not available; skipping domain age check")
        return None
    try:
        w = whois.whois(domain)
        creation = w.creation_date
        # creation_date may be list
        if isinstance(creation, list):
            creation = creation[0]
        if not creation:
            return None
        if isinstance(creation, str):
            creation = datetime.datetime.fromisoformat(creation)
        delta = datetime.datetime.now() - creation
        years = delta.days / 365.25
        return max(0.0, years)
    except Exception as e:
        logger.debug("whois lookup failed for %s: %s", domain, e)
        return None

def check_ssl_valid(domain: str, timeout: int = 5) -> Tuple[bool, Optional[str]]:
    """تحقق من وجود شهادة SSL صالحة (basic) — لا يتحقق من chain بالكامل"""
    try:
        ctx = ssl.create_default_context()
        with ctx.wrap_socket(socket.socket(), server_hostname=domain) as s:
            s.settimeout(timeout)
            s.connect((domain, 443))
            cert = s.getpeercert()
            # extract notBefore/notAfter
            not_after = cert.get("notAfter")
            return True, not_after
    except Exception as e:
        logger.debug("SSL check failed for %s: %s", domain, e)
        return False, None

def has_contact_and_about(url: str) -> Tuple[bool, bool]:
    """
    حاول الوصول إلى صفحات /contact أو /about أو البحث عن روابط contact/about في الصفحة الرئيسية.
    إرجاع (has_contact, has_about)
    """
    try:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        candidates = [f"{base}/contact", f"{base}/contact-us", f"{base}/about", f"{base}/about-us", base]
        headers = {"User-Agent": "MediaIntelCredChecker/1.0"}
        has_contact = False
        has_about = False
        for c in candidates:
            try:
                r = requests.get(c, headers=headers, timeout=6)
                if r.status_code != 200:
                    continue
                text = r.text.lower()
                if "contact" in c or "contact" in text:
                    has_contact = True
                if "about" in c or "about" in text or "editorial" in text:
                    has_about = True
                # if both found, break early
                if has_contact and has_about:
                    break
            except Exception:
                continue
        return has_contact, has_about
    except Exception as e:
        logger.debug("has_contact_and_about failed for %s: %s", url, e)
        return False, False

def external_reputation_score(domain: str) -> Optional[float]:
    """ارجع درجة سمعة خارجية إن وُجدت في القوائم المحملة"""
    try:
        return float(_external_reputation.get(domain, _external_reputation.get(domain.replace("www.", ""), 0.0)))
    except Exception:
        return None

# ---------------------------
# Scoring logic
# ---------------------------
def normalize_domain_age_score(years: Optional[float]) -> float:
    """تحويل عمر الدومين إلى درجة 0..1 (مثال بسيط)"""
    if years is None:
        return 0.5  # neutral if unknown
    # saturate at 10 years
    return min(1.0, years / 10.0)

def compute_score_components(domain: str, sample_url: Optional[str] = None, history_score: Optional[float] = None) -> Dict[str, Any]:
    """
    احسب مكونات الدرجة:
      - domain_age_score
      - ssl_valid (0/1) + expiry
      - contact/about presence
      - external_reputation (0..1)
      - history_score (0..1) إن وُجد (سجل دقة سابق)
    """
    details = {}
    # domain age
    age_years = get_domain_age_years(domain)
    details["domain_age_years"] = age_years
    details["domain_age_score"] = normalize_domain_age_score(age_years)

    # ssl
    ssl_ok, ssl_expiry = check_ssl_valid(domain)
    details["ssl_ok"] = bool(ssl_ok)
    details["ssl_expiry"] = ssl_expiry

    # contact/about
    if sample_url:
        has_contact, has_about = has_contact_and_about(sample_url)
    else:
        has_contact, has_about = False, False
    details["has_contact"] = has_contact
    details["has_about"] = has_about

    # external reputation
    ext_score = external_reputation_score(domain)
    details["external_reputation"] = ext_score if ext_score is not None else None

    # history (placeholder)
    details["history_score"] = history_score if history_score is not None else None

    # compute weighted score
    # default values for missing components: neutral 0.5
    domain_age_val = details["domain_age_score"] if details["domain_age_score"] is not None else 0.5
    ssl_val = 1.0 if details["ssl_ok"] else 0.0
    contact_val = 1.0 if details["has_contact"] else 0.0
    editorial_val = 1.0 if details["has_about"] else 0.0
    history_val = details["history_score"] if details["history_score"] is not None else 0.5
    external_val = details["external_reputation"] if details["external_reputation"] is not None else 0.5

    # normalize weights sum to 1
    total_w = WEIGHT_DOMAIN_AGE + WEIGHT_SSL + WEIGHT_CONTACT + WEIGHT_EDITORIAL + WEIGHT_HISTORY + WEIGHT_EXTERNAL
    if total_w == 0:
        total_w = 1.0
    score = (
        WEIGHT_DOMAIN_AGE * domain_age_val +
        WEIGHT_SSL * ssl_val +
        WEIGHT_CONTACT * contact_val +
        WEIGHT_EDITORIAL * editorial_val +
        WEIGHT_HISTORY * history_val +
        WEIGHT_EXTERNAL * external_val
    ) / total_w

    details["computed_score"] = float(score)
    details["weights"] = {
        "domain_age": WEIGHT_DOMAIN_AGE,
        "ssl": WEIGHT_SSL,
        "contact": WEIGHT_CONTACT,
        "editorial": WEIGHT_EDITORIAL,
        "history": WEIGHT_HISTORY,
        "external": WEIGHT_EXTERNAL
    }
    return details

# ---------------------------
# Public API
# ---------------------------
def compute_credibility_for_source(sample_url: str, history_score: Optional[float] = None) -> Dict[str, Any]:
    """
    الدالة الرئيسية التي يستدعيها Orchestrator أو Agent Analyzer.
    تُعيد dict بالشكل:
      {
        "domain": "...",
        "computed_at": iso_ts,
        "score": 0..1,
        "details": { ... explainability ... }
      }
    وتقوم بنشر النتيجة إلى Kafka وكتابة سجل في Postgres/MinIO إن مُمكّن.
    """
    domain = domain_from_url(sample_url)
    logger.info("Computing credibility for domain: %s (sample_url=%s)", domain, sample_url)
    details = compute_score_components(domain, sample_url, history_score)
    score = details.get("computed_score", 0.0)
    result = {
        "domain": domain,
        "sample_url": sample_url,
        "computed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "score": float(score),
        "details": details
    }

    # persist to Postgres
    try:
        if pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("""
                INSERT INTO source_credibility (domain, computed_at, score, details)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (domain) DO UPDATE
                SET computed_at = EXCLUDED.computed_at,
                    score = EXCLUDED.score,
                    details = EXCLUDED.details;
                """, (domain, result["computed_at"], result["score"], json.dumps(result["details"], ensure_ascii=False)))
    except Exception as e:
        logger.debug("Postgres write failed: %s", e)

    # store snapshot to MinIO
    try:
        if s3_client:
            key = f"credibility/{domain}/{int(time.time())}.json"
            s3_client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=json.dumps(result, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        logger.debug("MinIO write failed: %s", e)

    # publish to Kafka
    try:
        producer.produce(KAFKA_CRED_TOPIC, json.dumps(result, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.info("Published credibility for %s -> score=%.3f", domain, score)
    except Exception as e:
        logger.exception("Failed to publish credibility to Kafka: %s", e)

    return result

# ---------------------------
# CLI quick test
# ---------------------------
if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser(description="Compute credibility for a source URL")
    parser.add_argument("url", help="Sample URL from the source (e.g., https://example.com/article/123)")
    parser.add_argument("--history", type=float, default=None, help="Optional history score 0..1")
    args = parser.parse_args()
    res = compute_credibility_for_source(args.url, args.history)
    print(json.dumps(res, indent=2, ensure_ascii=False))
