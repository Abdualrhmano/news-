#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orchestrator/main_runner.py

الملف الرئيسي (Orchestrator Core) الذي يربط كل الوكلاء ويُدير دورة العمل.
المسؤوليات:
- تشغيل/إدارة موصلات الجمع (اختياري عبر subprocess)
- استهلاك سجلات canonical المجمعة من Kafka (topic: tech.deduped)
- تنسيق سلسلة المعالجة لكل canonical:
    1. حساب مقاييس الانتشار (propagation)
    2. حساب مصداقية المصدر (credibility)
    3. حساب أهمية الخبر (importance scoring)
    4. توليد المحتوى (RAG + LLM)
    5. نشر تلقائي (إذا كانت الشروط متوافرة)
- جدولة دورة كاملة كل 12 ساعة (configurable)
- تسجيل تدقيق (audit) وhealth endpoints بسيطة
- دعم التشغيل كخدمة محلية أو تشغيل كل المكونات في نفس العملية
ملاحظات:
- يعتمد على الوحدات الأخرى في المشروع (analysis.propagation, analysis.credibility,
  models.importance_scorer, content_gen.generator, publisher.publisher_selenium).
- كل استدعاء محاط بـtry/except لضمان عدم انهيار الحلقة الكاملة عند خطأ في عنصر واحد.
- قابل للتوسع: يمكن استبدال النشر عبر Selenium بـAPI adapters لاحقاً.
"""

import os
import sys
import time
import json
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

# Kafka client
from confluent_kafka import Consumer, Producer

# Attempt to import project modules; if غير متوفرين، نعمل fallback مع تحذير
try:
    from analysis.propagation import compute_propagation_metrics, publish_propagation, store_metrics_minio, store_metrics_postgres
except Exception:
    compute_propagation_metrics = None

try:
    from analysis.credibility import compute_credibility_for_source
except Exception:
    compute_credibility_for_source = None

try:
    from models.importance_scorer import predict_from_aggregated, publish_signal
except Exception:
    predict_from_aggregated = None

try:
    from content_gen.generator import generate_and_attach
except Exception:
    generate_and_attach = None

try:
    from publisher.publisher_selenium import SeleniumPublisher
except Exception:
    SeleniumPublisher = None

# ---------------------------
# Configuration via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEDUPED_TOPIC = os.getenv("DEDUPED_TOPIC", "tech.deduped")
PROPAGATION_TOPIC = os.getenv("PROPAGATION_TOPIC", "tech.propagation")
SIGNAL_TOPIC = os.getenv("SIGNAL_TOPIC", "tech.signals")
CREDIBILITY_TOPIC = os.getenv("KAFKA_CRED_TOPIC", "tech.credibility")

ORCHESTRATOR_GROUP = os.getenv("ORCHESTRATOR_GROUP", "orchestrator-group")
SPAWN_CONNECTORS = os.getenv("SPAWN_CONNECTORS", "false").lower() == "true"
CONNECTORS_TO_SPAWN = os.getenv("CONNECTORS_TO_SPAWN", "connectors/snn_connector.py,connectors/makroohsoft_connector.py").split(",")
CONNECTOR_PYTHON = os.getenv("CONNECTOR_PYTHON", sys.executable)

SCHEDULE_INTERVAL_HOURS = int(os.getenv("SCHEDULE_INTERVAL_HOURS", "12"))
WORKER_THREADS = int(os.getenv("ORCHESTRATOR_WORKERS", "4"))
PUBLISH_PLATFORMS = os.getenv("PUBLISH_PLATFORMS", "x,facebook,internal").split(",")  # default platforms
PUBLISHER_PROFILE_DIR = os.getenv("SELENIUM_PROFILE_DIR", "/tmp/selenium_profile")

LOG_LEVEL = os.getenv("ORCHESTRATOR_LOG_LEVEL", "INFO")
AUDIT_LOG_PATH = os.getenv("ORCHESTRATOR_AUDIT_LOG", "logs/orchestrator_audit.log")

# ---------------------------
# Logging setup
# ---------------------------
os.makedirs(os.path.dirname(AUDIT_LOG_PATH) or ".", exist_ok=True)
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [orchestrator] %(message)s")
logger = logging.getLogger("orchestrator")
audit_logger = logging.getLogger("orchestrator_audit")
audit_handler = logging.FileHandler(AUDIT_LOG_PATH, encoding="utf-8")
audit_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
audit_logger.addHandler(audit_handler)
audit_logger.propagate = False

# ---------------------------
# Kafka clients
# ---------------------------
consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": ORCHESTRATOR_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}
consumer = Consumer(consumer_conf)
consumer.subscribe([DEDUPED_TOPIC])

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# ---------------------------
# Process management for connectors (optional)
# ---------------------------
_connector_processes: List[Any] = []
_shutdown_flag = threading.Event()

def spawn_connectors():
    """Spawn connector scripts as subprocesses (optional)."""
    import subprocess
    global _connector_processes
    logger.info("Spawning connectors: %s", CONNECTORS_TO_SPAWN)
    for script in CONNECTORS_TO_SPAWN:
        script = script.strip()
        if not script:
            continue
        try:
            p = subprocess.Popen([CONNECTOR_PYTHON, script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _connector_processes.append((script, p))
            logger.info("Spawned connector %s (pid=%s)", script, p.pid)
        except Exception as e:
            logger.exception("Failed to spawn connector %s: %s", script, e)

def stop_connectors():
    """Terminate spawned connector subprocesses gracefully."""
    import signal
    for script, p in _connector_processes:
        try:
            logger.info("Terminating connector %s (pid=%s)", script, getattr(p, "pid", "<unknown>"))
            p.terminate()
            try:
                p.wait(timeout=10)
            except Exception:
                p.kill()
        except Exception as e:
            logger.debug("Error terminating connector %s: %s", script, e)

# ---------------------------
# Utility helpers
# ---------------------------
def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def audit_record(action: str, payload: Dict[str, Any]):
    """سجل تدقيق مبسط: يكتب JSON إلى ملف لسهولة المراجعة"""
    rec = {"action": action, "ts": now_iso(), "payload": payload}
    try:
        audit_logger.info(json.dumps(rec, ensure_ascii=False))
    except Exception:
        logger.debug("Failed to write audit record")

# ---------------------------
# Pipeline step wrappers
# ---------------------------
def compute_and_publish_propagation(aggregated: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """حساب propagation metrics ونشرها إلى Kafka (إن أمكن)"""
    if compute_propagation_metrics is None:
        logger.warning("Propagation module not available")
        return None
    try:
        metrics = compute_propagation_metrics(aggregated)
        # publish via producer to PROPAGATION_TOPIC
        try:
            producer.produce(PROPAGATION_TOPIC, json.dumps(metrics, ensure_ascii=False).encode("utf-8"))
            producer.poll(0)
        except Exception as e:
            logger.debug("Failed to publish propagation metrics to Kafka: %s", e)
        audit_record("propagation_computed", {"canonical_id": aggregated.get("canonical_id"), "metrics_summary": {"total_mentions": metrics.get("total_mentions")}})
        return metrics
    except Exception as e:
        logger.exception("Propagation computation failed for %s: %s", aggregated.get("canonical_id"), e)
        return None

def compute_credibility(sample_url: str) -> Optional[Dict[str, Any]]:
    """حساب مصداقية المصدر"""
    if compute_credibility_for_source is None:
        logger.warning("Credibility module not available")
        return None
    try:
        cred = compute_credibility_for_source(sample_url)
        audit_record("credibility_computed", {"domain": cred.get("domain"), "score": cred.get("score")})
        return cred
    except Exception as e:
        logger.exception("Credibility computation failed for %s: %s", sample_url, e)
        return None

def score_importance(aggregated: Dict[str, Any], propagation_metrics: Optional[Dict[str, Any]], credibility: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """تشغيل نموذج الأهمية وإرجاع signal"""
    if predict_from_aggregated is None:
        logger.warning("Importance scorer not available")
        return None
    try:
        # ensure aggregated contains canonical_id
        if "canonical_id" not in aggregated:
            aggregated["canonical_id"] = aggregated.get("canonical_link") or aggregated.get("title", "")[:40]
        signal = predict_from_aggregated(aggregated, propagation_metrics, credibility)
        # publish signal to Kafka for downstream agents
        try:
            producer.produce(SIGNAL_TOPIC, json.dumps(signal, ensure_ascii=False).encode("utf-8"))
            producer.poll(0)
        except Exception as e:
            logger.debug("Failed to publish signal to Kafka: %s", e)
        audit_record("importance_scored", {"canonical_id": signal.get("canonical_id"), "score": signal.get("importance_score")})
        return signal
    except Exception as e:
        logger.exception("Importance scoring failed for %s: %s", aggregated.get("canonical_id"), e)
        return None

def generate_content_and_decide(signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """توليد المحتوى عبر LLM/RAG وتحديد جاهزية النشر"""
    if generate_and_attach is None:
        logger.warning("Content generator not available")
        return None
    try:
        updated = generate_and_attach(signal)
        audit_record("content_generated", {"canonical_id": updated.get("canonical_id"), "publish_ready": updated.get("publish_ready")})
        return updated
    except Exception as e:
        logger.exception("Content generation failed for %s: %s", signal.get("canonical_id"), e)
        return None

def publish_signal_content(signal: Dict[str, Any], publisher: Any) -> Dict[str, Any]:
    """
    نشر المحتوى إلى المنصات المحددة.
    publisher: instance of SeleniumPublisher أو أي adapter يرث PublisherBase
    """
    result_summary = {"canonical_id": signal.get("canonical_id"), "published": []}
    variants = signal.get("content_variants", {})
    metadata = {"canonical_id": signal.get("canonical_id"), "importance_score": signal.get("importance_score")}
    # نشر لكل منصة في PUBLISH_PLATFORMS
    for platform in PUBLISH_PLATFORMS:
        try:
            payload = {"platform": platform.strip(), "metadata": metadata}
            if platform.strip() == "x":
                payload["tweet"] = variants.get("tweet") or variants.get("summary")[:250]
            elif platform.strip() == "facebook":
                payload["fb_post"] = variants.get("fb_post") or variants.get("summary")
            elif platform.strip() == "internal":
                payload["text"] = variants.get("summary")
            else:
                # unknown platform: skip
                logger.debug("Unknown platform configured: %s", platform)
                continue
            res = publisher.publish_with_retry(payload)
            result_summary["published"].append({"platform": platform, "status": res.get("status"), "platform_id": res.get("platform_id")})
            audit_record("published", {"canonical_id": signal.get("canonical_id"), "platform": platform, "status": res.get("status")})
        except Exception as e:
            logger.exception("Publishing to %s failed for %s: %s", platform, signal.get("canonical_id"), e)
            result_summary["published"].append({"platform": platform, "status": "failed", "error": str(e)})
    return result_summary

# ---------------------------
# Main processing loop for canonical records
# ---------------------------
def process_canonical_pipeline(record: Dict[str, Any], publisher_instance: Optional[Any] = None) -> Dict[str, Any]:
    """
    تنسيق كامل لمعالجة سجل canonical واحد:
    - حساب propagation
    - حساب credibility (باستخدام أول رابط من mentions)
    - importance scoring
    - content generation
    - publish (إذا جاهز)
    يعيد dict ملخّص بالنتائج.
    """
    summary = {"canonical_id": record.get("canonical_id"), "status": "started", "steps": {}}
    aggregated = record.get("aggregated", {}) or {}
    # ensure canonical_id in aggregated
    aggregated["canonical_id"] = record.get("canonical_id") or aggregated.get("canonical_link")

    # 1. propagation
    try:
        propagation_metrics = compute_and_publish_propagation(aggregated)
        summary["steps"]["propagation"] = {"ok": propagation_metrics is not None}
    except Exception as e:
        logger.exception("Propagation step failed: %s", e)
        propagation_metrics = None
        summary["steps"]["propagation"] = {"ok": False, "error": str(e)}

    # 2. credibility: pick a sample URL (canonical_link or first mention)
    sample_url = aggregated.get("canonical_link")
    if not sample_url:
        mentions = aggregated.get("mentions", [])
        sample_url = mentions[0].get("link") if mentions else None
    try:
        credibility = compute_credibility(sample_url) if sample_url else None
        summary["steps"]["credibility"] = {"ok": credibility is not None}
    except Exception as e:
        logger.exception("Credibility step failed: %s", e)
        credibility = None
        summary["steps"]["credibility"] = {"ok": False, "error": str(e)}

    # 3. importance scoring
    try:
        signal = score_importance(aggregated, propagation_metrics, credibility)
        summary["steps"]["importance"] = {"ok": signal is not None, "score": signal.get("importance_score") if signal else None}
    except Exception as e:
        logger.exception("Importance step failed: %s", e)
        signal = None
        summary["steps"]["importance"] = {"ok": False, "error": str(e)}

    # 4. content generation
    try:
        if signal:
            # attach propagation and credibility into signal for generator context
            signal["propagation_metrics"] = propagation_metrics
            signal["credibility"] = credibility
            generated = generate_content_and_decide(signal)
            summary["steps"]["generation"] = {"ok": generated is not None, "publish_ready": generated.get("publish_ready") if generated else False}
        else:
            generated = None
            summary["steps"]["generation"] = {"ok": False, "reason": "no_signal"}
    except Exception as e:
        logger.exception("Generation step failed: %s", e)
        generated = None
        summary["steps"]["generation"] = {"ok": False, "error": str(e)}

    # 5. publishing (automatic)
    publish_result = None
    try:
        if generated and generated.get("publish_ready") and publisher_instance:
            publish_result = publish_signal_content(generated, publisher_instance)
            summary["steps"]["publish"] = {"ok": True, "result": publish_result}
        else:
            summary["steps"]["publish"] = {"ok": False, "reason": "not_ready_or_no_publisher"}
    except Exception as e:
        logger.exception("Publish step failed: %s", e)
        summary["steps"]["publish"] = {"ok": False, "error": str(e)}

    summary["status"] = "done"
    audit_record("pipeline_completed", {"canonical_id": summary.get("canonical_id"), "summary": summary["steps"]})
    return summary

# ---------------------------
# Worker pool loop: consume deduped topic and process messages
# ---------------------------
def run_orchestrator_loop(worker_count: int = WORKER_THREADS):
    logger.info("Orchestrator loop starting. Subscribed to %s. Workers=%d", DEDUPED_TOPIC, worker_count)
    # instantiate publisher instance (Selenium) if available
    publisher_instance = None
    if SeleniumPublisher:
        try:
            publisher_instance = SeleniumPublisher(name="selenium")
        except Exception as e:
            logger.warning("Failed to init SeleniumPublisher: %s", e)
            publisher_instance = None
    else:
        logger.info("SeleniumPublisher not available; publishing disabled until adapter provided.")

    executor = ThreadPoolExecutor(max_workers=worker_count)
    futures = set()

    try:
        while not _shutdown_flag.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Kafka consumer error: %s", msg.error())
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                logger.exception("Failed to decode Kafka message: %s", e)
                continue

            # submit pipeline job
            future = executor.submit(process_canonical_pipeline, payload, publisher_instance)
            futures.add(future)

            # cleanup completed futures to avoid memory growth
            done = {f for f in futures if f.done()}
            for f in done:
                try:
                    res = f.result()
                    logger.info("Pipeline finished for %s", res.get("canonical_id"))
                except Exception as e:
                    logger.exception("Pipeline job raised: %s", e)
                futures.remove(f)

    except KeyboardInterrupt:
        logger.info("Orchestrator interrupted by user.")
    finally:
        logger.info("Shutting down orchestrator loop: waiting for running jobs to finish.")
        _shutdown_flag.set()
        executor.shutdown(wait=True)
        if publisher_instance:
            try:
                publisher_instance.close()
            except Exception:
                pass
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Orchestrator stopped.")

# ---------------------------
# Scheduler: trigger connectors and/or full-cycle tasks every N hours
# ---------------------------
def scheduler_loop(interval_hours: int = SCHEDULE_INTERVAL_HOURS):
    """
    بسيط: كل interval_hours يقوم بتشغيل دورة:
      - (اختياري) spawn connectors لفترة قصيرة لجمع الأخبار
      - ينتظر بعض الوقت للسماح بالـingestion
      - لا حاجة لأن يقوم Orchestrator بنفسه بعملية dedupe لأن deduper يكتب إلى topic deduped
    ملاحظة: هذا مجرد مساعد لتشغيل الموصلات بشكل مجدول؛ المعالجة الفعلية تتم عبر استهلاك topic deduped.
    """
    logger.info("Scheduler started: interval %d hours", interval_hours)
    while not _shutdown_flag.is_set():
        start = datetime.utcnow()
        logger.info("Scheduler tick at %s: spawning connectors (if configured).", start.isoformat())
        if SPAWN_CONNECTORS:
            spawn_connectors()
            # اترك الموصلات تعمل لفترة قصيرة ثم أوقفها
            wait_seconds = int(os.getenv("CONNECTOR_RUN_SECONDS", "300"))  # default 5 minutes
            logger.info("Connectors spawned; will run for %d seconds then terminate.", wait_seconds)
            time.sleep(wait_seconds)
            stop_connectors()
        else:
            logger.debug("SPAWN_CONNECTORS disabled; skipping spawn.")

        # sleep until next interval
        next_run = start + timedelta(hours=interval_hours)
        sleep_seconds = max(0, (next_run - datetime.utcnow()).total_seconds())
        logger.info("Scheduler sleeping for %.0f seconds until next tick (%s).", sleep_seconds, next_run.isoformat())
        # break sleep into chunks to be responsive to shutdown
        slept = 0
        while slept < sleep_seconds and not _shutdown_flag.is_set():
            time.sleep(min(30, sleep_seconds - slept))
            slept += min(30, sleep_seconds - slept)

    logger.info("Scheduler loop exiting due to shutdown flag.")

# ---------------------------
# Signal handling for graceful shutdown
# ---------------------------
def _signal_handler(signum, frame):
    logger.info("Received signal %s: initiating graceful shutdown.", signum)
    _shutdown_flag.set()

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------
# CLI / Entrypoint
# ---------------------------
def main():
    """
    تشغيل Orchestrator:
    - يبدأ scheduler في thread منفصل (اختياري)
    - يبدأ حلقة المعالجة الرئيسية التي تستهلك topic deduped
    """
    logger.info("Orchestrator starting up. Kafka=%s DEDUPED_TOPIC=%s", KAFKA_BOOTSTRAP, DEDUPED_TOPIC)

    # start scheduler thread
    scheduler_thread = threading.Thread(target=scheduler_loop, args=(SCHEDULE_INTERVAL_HOURS,), daemon=True)
    scheduler_thread.start()

    # start main orchestrator loop (blocking)
    run_orchestrator_loop(worker_count=WORKER_THREADS)

    # wait for scheduler to finish
    scheduler_thread.join(timeout=5)
    logger.info("Orchestrator main finished. Exiting.")

if __name__ == "__main__":
    main()
