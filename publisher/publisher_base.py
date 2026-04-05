#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
publisher/publisher_base.py

الواجهة الأساسية (abstract) لجميع adapters النشر.
كل adapter (X, Facebook, Instagram, internal) يرث من هذه الفئة ويطبق الدالة publish.
تضمن التصميم:
- واجهة موحدة للنشر (publish)
- دعم retry مع exponential backoff
- تسجيل audit log موحد (يمكن ربطه بقاعدة بيانات أو Kafka)
- استثناءات واضحة للتعامل مع حالات الفشل المؤقت والدائم
- إمكانية استبدال Selenium بـAPI رسمية لاحقاً دون تغيير بقية النظام
"""

from typing import Dict, Any, Optional
import time
import logging
import json
import os
import requests

# Logging
logging.basicConfig(level=os.getenv("PUBLISHER_LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s [publisher_base] %(message)s")
logger = logging.getLogger("publisher_base")

# Simple retry decorator
def retry_on_exception(max_retries: int = 3, backoff_base: float = 2.0, allowed_exceptions: tuple = (Exception,)):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except allowed_exceptions as e:
                    attempt += 1
                    if attempt > max_retries:
                        logger.exception("Max retries reached for %s: %s", fn.__name__, e)
                        raise
                    sleep_for = (backoff_base ** attempt) + (0.1 * attempt)
                    logger.warning("Retry %d/%d for %s after %.2fs due to: %s", attempt, max_retries, fn.__name__, sleep_for, e)
                    time.sleep(sleep_for)
        return wrapper
    return decorator

class PublishError(Exception):
    """خطأ عام للنشر"""
    pass

class PermanentPublishError(PublishError):
    """خطأ دائم (مثلاً: مصادقة خاطئة أو محتوى محظور)"""
    pass

class TransientPublishError(PublishError):
    """خطأ مؤقت (شبكة، rate limit مؤقت)"""
    pass

class PublisherBase:
    """
    الفئة الأساسية: كل adapter يرث منها.
    يجب أن يطبق child class الدالة publish(payload: Dict) -> Dict (response info).
    payload: يحتوي على الحقول التالية المتوقعة:
      - text (str) أو tweet
      - image_url (optional)
      - link
      - metadata (dict)  # signal id, canonical_id, source, generated_at
    """
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"publisher.{name}")

    @retry_on_exception(max_retries=3, backoff_base=2.0, allowed_exceptions=(TransientPublishError,))
    def publish_with_retry(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """غلاف retry يدير الأخطاء المؤقتة"""
        return self.publish(payload)

    def publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        يجب أن تطبّق في الفئات الفرعية.
        ترجع dict يحتوي على:
          - status: "ok" | "failed"
          - platform_id: external post id إن وُجد
          - response: raw response أو رسالة خطأ
        """
        raise NotImplementedError("publish must be implemented by subclass")

    def audit_record(self, payload: Dict[str, Any], result: Dict[str, Any]):
        """
        سجل بسيط: يمكن استبداله بكتابة إلى Postgres أو Kafka.
        هنا نطبع JSON إلى اللوج كنسخة أولية.
        """
        rec = {
            "platform": self.name,
            "payload_meta": payload.get("metadata", {}),
            "status": result.get("status"),
            "platform_id": result.get("platform_id"),
            "response_summary": str(result.get("response"))[:500],
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        self.logger.info("AUDIT: %s", json.dumps(rec, ensure_ascii=False))

    def validate_payload(self, payload: Dict[str, Any]):
        """تحقق أساسي من الحقول المطلوبة"""
        if not isinstance(payload, dict):
            raise PermanentPublishError("payload must be a dict")
        if not payload.get("text") and not payload.get("tweet") and not payload.get("fb_post"):
            raise PermanentPublishError("No text content provided for publishing")
