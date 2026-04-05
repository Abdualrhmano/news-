#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
utils/paywall_detector.py
وحدة كشف الحواجز المدفوعة (paywall) للمقالات والصفحات الإخبارية.
الهدف: إعطاء قرار سريع وموثوق إلى حد معقول إن كانت صفحة الخبر محمية بمحتوى مدفوع أو لا.
المخرجات: دالة is_paywalled(url) تُعيد (bool, details) حيث details dict يحتوي أسباب القرار وإشارات داعمة.
ملاحظات تصميمية:
- تعتمد على إشارات متعددة (status code, content-length, كلمات مفتاحية، عناصر DOM شائعة في paywalls، redirects، cookies).
- تعمل كفحص أولي سريع؛ ليست بديلاً عن فحص قانوني أو اتفاق ترخيص.
- قابلة للتوسيع: يمكن لاحقاً إضافة ML-based detector أو قواعد خاصة بمواقع معينة.
- لا تقوم بكسر سياسات المواقع: تحترم robots.txt قبل أي سحب (يفترض أن الموصل يتحقق أيضاً).
"""

from typing import Tuple, Dict, Optional
import os
import re
import time
import logging
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

# Configuration via env
REQUEST_TIMEOUT = int(os.getenv("PAYWALL_REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("PAYWALL_MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("PAYWALL_BACKOFF_BASE", "1.5"))
USER_AGENT = os.getenv("PAYWALL_USER_AGENT", "MediaIntelPaywallChecker/1.0 (+https://example.com)")
LOG_LEVEL = os.getenv("PAYWALL_LOG_LEVEL", "INFO")

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [paywall_detector] %(message)s")
logger = logging.getLogger("paywall_detector")

# Heuristics lists (قابلة للتعديل أو التحميل من ملف خارجي)
PAYWALL_KEYWORDS = [
    "subscribe to read", "subscription required", "subscribe now", "sign up to read",
    "this content is for subscribers", "you must be a subscriber", "paywall",
    "join now to continue reading", "become a member", "members only", "premium content",
    "content is locked", "read more with a subscription", "subscribe for unlimited access",
    "please subscribe", "enter your email to continue"
]
PAYWALL_CSS_SELECTORS = [
    ".paywall", ".subscription-wall", ".paywall-overlay", ".paywall__content",
    ".meteredContent", ".article-paywall", "#paywall", ".paywall-gate"
]
SHORT_CONTENT_THRESHOLD = int(os.getenv("PAYWALL_SHORT_CONTENT_THRESHOLD", "300"))  # characters
MIN_VISIBLE_TEXT_RATIO = float(os.getenv("PAYWALL_MIN_VISIBLE_TEXT_RATIO", "0.25"))  # visible text vs total text

# Some domains have known paywall patterns; can be extended
KNOWN_PAYWALLED_DOMAINS = {
    # example: "examplepaywalled.com": "metered",
}

# ---------------------------
# Helpers
# ---------------------------
def is_allowed_by_robots(url: str, user_agent: str = USER_AGENT) -> bool:
    """تحقق سريع من robots.txt للمجال"""
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
        logger.warning("robots.txt check failed for %s: %s. Defaulting to allow.", url, e)
        return True

def safe_get(url: str, headers: dict = None, timeout: int = REQUEST_TIMEOUT) -> Optional[requests.Response]:
    """GET مع retry وexponential backoff"""
    headers = headers or {"User-Agent": USER_AGENT}
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            return resp
        except requests.RequestException as e:
            attempt += 1
            sleep_for = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.5)
            logger.debug("Request attempt %d failed for %s: %s. Backoff %.2fs", attempt, url, e, sleep_for)
            time.sleep(sleep_for)
    logger.error("All GET retries failed for %s", url)
    return None

def text_length_ratio(soup: BeautifulSoup) -> float:
    """نسبة النص المرئي إلى النص الكلي (تقريبية)"""
    try:
        texts = soup.stripped_strings
        visible_text = " ".join(list(texts))
        total_len = len(visible_text)
        # raw HTML length
        raw_len = len(str(soup))
        if raw_len == 0:
            return 0.0
        return total_len / raw_len
    except Exception:
        return 0.0

def contains_paywall_keywords(text: str) -> Tuple[bool, list]:
    """تحقق من وجود كلمات مفتاحية مرتبطة بالـpaywall"""
    found = []
    lower = text.lower()
    for kw in PAYWALL_KEYWORDS:
        if kw in lower:
            found.append(kw)
    return (len(found) > 0, found)

def has_paywall_selectors(soup: BeautifulSoup) -> Tuple[bool, list]:
    """تحقق من وجود عناصر DOM شائعة للـpaywall"""
    found = []
    for sel in PAYWALL_CSS_SELECTORS:
        try:
            if soup.select_one(sel):
                found.append(sel)
        except Exception:
            continue
    return (len(found) > 0, found)

def is_short_content(soup: BeautifulSoup) -> Tuple[bool, int]:
    """تحقق إن المحتوى قصير جداً (قد يكون مقتطفاً بسبب paywall)"""
    texts = list(soup.stripped_strings)
    visible_text = " ".join(texts)
    length = len(visible_text)
    return (length < SHORT_CONTENT_THRESHOLD, length)

# ---------------------------
# Main detection functions
# ---------------------------
def detect_paywall_from_html(html: str, url: str = "") -> Dict:
    """
    تحليل HTML وإرجاع إشارات paywall مفصّلة.
    النتيجة: dict يحتوي على مفاتيح:
      - paywalled (bool)
      - reasons (list of strings)
      - indicators (detailed dict)
    """
    indicators = {}
    try:
        soup = BeautifulSoup(html, "html.parser")
        # 1. كلمات مفتاحية
        text = soup.get_text(" ", strip=True)[:5000]  # فحص أولي
        kw_flag, kw_list = contains_paywall_keywords(text)
        indicators["keyword_matches"] = kw_list

        # 2. selectors
        sel_flag, sel_list = has_paywall_selectors(soup)
        indicators["selector_matches"] = sel_list

        # 3. short content
        short_flag, length = is_short_content(soup)
        indicators["visible_text_length"] = length
        indicators["short_content_flag"] = short_flag

        # 4. visible text ratio
        ratio = text_length_ratio(soup)
        indicators["visible_text_ratio"] = ratio

        # 5. presence of login/subscribe forms
        login_forms = []
        for form in soup.find_all("form"):
            form_text = form.get_text(" ", strip=True).lower()
            if "login" in form_text or "subscribe" in form_text or "register" in form_text:
                login_forms.append(form_text[:200])
        indicators["login_forms_count"] = len(login_forms)

        # 6. paywall scripts or classes in HTML
        script_text = " ".join([s.get_text(" ", strip=True) for s in soup.find_all("script")[:10]])
        script_flag = False
        if "paywall" in script_text.lower() or "subscription" in script_text.lower():
            script_flag = True
        indicators["paywall_script_flag"] = script_flag

        # Decision heuristics (قواعد بسيطة مبدئية)
        score = 0
        reasons = []
        if kw_flag:
            score += 3
            reasons.append("paywall keywords found")
        if sel_flag:
            score += 3
            reasons.append("paywall selectors found")
        if short_flag:
            score += 2
            reasons.append("visible content is very short")
        if ratio < MIN_VISIBLE_TEXT_RATIO:
            score += 1
            reasons.append("low visible text ratio")
        if indicators.get("login_forms_count", 0) > 0:
            score += 1
            reasons.append("login/subscribe form present")
        if script_flag:
            score += 1
            reasons.append("paywall-related scripts detected")

        # threshold: إذا كان score >= 4 نعتبرها paywalled (قابل للتعديل)
        paywalled = score >= 4

        return {
            "paywalled": paywalled,
            "score": score,
            "reasons": reasons,
            "indicators": indicators
        }
    except Exception as e:
        logger.exception("Error in detect_paywall_from_html: %s", e)
        return {"paywalled": False, "score": 0, "reasons": ["error"], "indicators": {}}

def is_paywalled(url: str) -> Tuple[bool, Dict]:
    """
    الدالة الرئيسية التي يستدعيها الموصل.
    تُعيد (paywalled_bool, details_dict).
    تفاصيل العملية:
      1. تحقق robots.txt (لا تتجاوز القيود)
      2. GET الصفحة مع retries
      3. فحص status code وredirects
      4. تحليل HTML عبر detect_paywall_from_html
      5. بعض قواعد domain-specific (قابلة للتوسيع)
    """
    details = {
        "url": url,
        "checked_at": time.time(),
        "http_status": None,
        "redirect_chain": [],
        "error": None
    }

    # robots check
    try:
        if not is_allowed_by_robots(url):
            details["error"] = "disallowed_by_robots"
            logger.info("Robots disallow fetching %s", url)
            # نعتبرها غير paywalled لكن نعلم الموصل أن robots منعت الفحص
            return (False, details)
    except Exception as e:
        logger.debug("robots check error: %s", e)

    # domain quick check
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain in KNOWN_PAYWALLED_DOMAINS:
            details["http_status"] = "known_paywalled_domain"
            details["reason"] = KNOWN_PAYWALLED_DOMAINS[domain]
            logger.info("Domain %s is known paywalled (%s)", domain, KNOWN_PAYWALLED_DOMAINS[domain])
            return (True, details)
    except Exception:
        pass

    # fetch page
    resp = safe_get(url)
    if resp is None:
        details["error"] = "fetch_failed"
        return (False, details)

    details["http_status"] = resp.status_code
    # record redirect chain
    try:
        details["redirect_chain"] = [r.url for r in resp.history] + [resp.url]
    except Exception:
        details["redirect_chain"] = []

    # status code heuristics
    if resp.status_code in (401, 403):
        details["error"] = f"http_{resp.status_code}"
        logger.info("HTTP %s for %s — likely restricted", resp.status_code, url)
        return (True, details)

    # content-length heuristics
    content_len = len(resp.content) if resp.content else 0
    details["content_length"] = content_len
    if content_len < 500:  # صفحة قصيرة جداً
        details["reason_short_content"] = True

    # analyze HTML
    analysis = detect_paywall_from_html(resp.text, url)
    details.update({"analysis": analysis})

    # final decision
    paywalled = bool(analysis.get("paywalled", False))
    return (paywalled, details)

# ---------------------------
# CLI quick test
# ---------------------------
if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser(description="Paywall detector quick test")
    parser.add_argument("url", help="URL to check for paywall")
    args = parser.parse_args()
    url = args.url
    pw, details = is_paywalled(url)
    print(json.dumps({"url": url, "paywalled": pw, "details": details}, indent=2, ensure_ascii=False))
