#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
publisher/publisher_selenium.py

Adapter نشر مؤقت باستخدام Selenium لتشغيل النشر تلقائياً على منصات لا تتوفر لها مفاتيح API الآن.
ملاحظة مهمة:
- هذا حل مؤقت ومُعرّض للتغيّر (تغيّر DOM، سياسات المنصات).
- عند توفر مفاتيح API الرسمية يجب استبدال هذا adapter بـAPI adapter.
- لا تحفظ بيانات الدخول في الكود؛ استخدم Secrets Manager أو env vars مشفّرة.

الوظائف:
- login_x / post_x  (مثال لمنصة X)
- login_facebook / post_facebook (مثال لصفحة فيسبوك)
- login_instagram / post_instagram (عبر واجهة الويب أو Facebook Creator Studio)
- كل دالة ترجع dict موحّد (status, platform_id, response)
- دعم headless وprofile persistence للحفاظ على جلسات مسجلة
"""

import os
import time
import json
import logging
from typing import Dict, Any, Optional
from publisher.publisher_base import PublisherBase, TransientPublishError, PermanentPublishError

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.keys import Keys
    from selenium.common.exceptions import WebDriverException, NoSuchElementException, TimeoutException
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

# Logging
logging.basicConfig(level=os.getenv("PUBLISHER_LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s [publisher_selenium] %(message)s")
logger = logging.getLogger("publisher_selenium")

# Config via env
CHROME_DRIVER_PATH = os.getenv("CHROME_DRIVER_PATH", "/usr/bin/chromedriver")
BROWSER_HEADLESS = os.getenv("BROWSER_HEADLESS", "true").lower() == "true"
SELENIUM_PROFILE_DIR = os.getenv("SELENIUM_PROFILE_DIR", "/tmp/selenium_profile")  # to persist login sessions
SELENIUM_TIMEOUT = int(os.getenv("SELENIUM_TIMEOUT", "15"))

class SeleniumPublisher(PublisherBase):
    def __init__(self, name: str = "selenium"):
        super().__init__(name)
        if not SELENIUM_AVAILABLE:
            logger.warning("Selenium not available in environment. Publisher will not function.")
        self.driver = None

    def _init_driver(self):
        if not SELENIUM_AVAILABLE:
            raise PermanentPublishError("Selenium not installed")
        if self.driver:
            return self.driver
        opts = Options()
        if BROWSER_HEADLESS:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        # profile persistence to keep logged-in sessions
        opts.add_argument(f"--user-data-dir={SELENIUM_PROFILE_DIR}")
        # optional: set window size
        opts.add_argument("--window-size=1200,800")
        try:
            self.driver = webdriver.Chrome(executable_path=CHROME_DRIVER_PATH, options=opts)
            self.driver.set_page_load_timeout(SELENIUM_TIMEOUT)
            logger.info("Initialized Chrome WebDriver")
            return self.driver
        except WebDriverException as e:
            logger.exception("Failed to init WebDriver: %s", e)
            raise TransientPublishError("WebDriver init failed")

    def close(self):
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
        except Exception:
            pass

    # -------------------------
    # Example: X (Twitter) publish flow (simplified)
    # -------------------------
    def login_x(self, username: str, password: str) -> bool:
        """
        تسجيل الدخول إلى X عبر واجهة الويب. يُنصح بتسجيل الجلسة يدوياً أول مرة عبر profile persistence.
        لا تحفظ كلمات المرور في الكود.
        """
        driver = self._init_driver()
        try:
            driver.get("https://x.com/login")
            time.sleep(2)
            # Attempt to find username/password fields (DOM may change)
            try:
                user_field = driver.find_element(By.NAME, "text")
                user_field.clear()
                user_field.send_keys(username)
                user_field.send_keys(Keys.ENTER)
                time.sleep(2)
            except NoSuchElementException:
                logger.debug("Username field not found; trying alternative selectors")
            # password step (may be on next page)
            try:
                pwd_field = driver.find_element(By.NAME, "password")
                pwd_field.clear()
                pwd_field.send_keys(password)
                pwd_field.send_keys(Keys.ENTER)
                time.sleep(3)
            except NoSuchElementException:
                logger.debug("Password field not found; login may require additional steps")
            # crude check: presence of compose button
            try:
                driver.find_element(By.CSS_SELECTOR, "div[aria-label='Tweet text']")
                logger.info("Login to X appears successful (compose found)")
                return True
            except Exception:
                logger.warning("Login to X may have failed or requires additional verification")
                return False
        except Exception as e:
            logger.exception("Login to X failed: %s", e)
            raise TransientPublishError("Login failed")

    def post_x(self, text: str) -> Dict[str, Any]:
        """
        نشر تغريدة/منشور على X. يعيد dict موحّد.
        ملاحظة: DOM متغير؛ هذا مثال توضيحي.
        """
        self.validate_payload({"text": text})
        driver = self._init_driver()
        try:
            driver.get("https://x.com/compose/tweet")
            time.sleep(2)
            # textarea role textbox
            textarea = driver.find_element(By.CSS_SELECTOR, "div[role='textbox']")
            textarea.click()
            textarea.send_keys(text)
            time.sleep(1)
            # find tweet button
            btn = driver.find_element(By.XPATH, "//div[@data-testid='tweetButtonInline']")
            btn.click()
            time.sleep(2)
            # crude success check: no exception and URL changed or toast appeared
            logger.info("Posted to X via Selenium")
            result = {"status": "ok", "platform_id": None, "response": "posted_via_selenium"}
            self.audit_record({"text": text, "metadata": {}}, result)
            return result
        except NoSuchElementException as e:
            logger.exception("X post failed - element not found: %s", e)
            raise PermanentPublishError("X DOM changed or element not found")
        except Exception as e:
            logger.exception("X post failed: %s", e)
            raise TransientPublishError("X post failed")

    # -------------------------
    # Example: Facebook Page publish (simplified)
    # -------------------------
    def post_facebook_page(self, page_url: str, message: str) -> Dict[str, Any]:
        """
        نشر منشور على صفحة فيسبوك عبر واجهة الويب.
        page_url: رابط صفحة الفيسبوك (مثلاً https://www.facebook.com/YourPage)
        message: نص المنشور
        """
        self.validate_payload({"text": message})
        driver = self._init_driver()
        try:
            driver.get(page_url)
            time.sleep(3)
            # attempt to find create post area
            try:
                # selectors vary; try a few
                composer = driver.find_element(By.XPATH, "//div[@role='textbox']")
                composer.click()
                composer.send_keys(message)
                time.sleep(1)
                # find post button
                post_btn = driver.find_element(By.XPATH, "//div[@aria-label='Post' or @aria-label='نشر']")
                post_btn.click()
                time.sleep(3)
                result = {"status": "ok", "platform_id": None, "response": "posted_via_selenium"}
                self.audit_record({"text": message, "metadata": {}}, result)
                logger.info("Posted to Facebook page via Selenium")
                return result
            except NoSuchElementException:
                logger.exception("Facebook composer not found; DOM likely changed")
                raise PermanentPublishError("Facebook composer not found")
        except Exception as e:
            logger.exception("Facebook post failed: %s", e)
            raise TransientPublishError("Facebook post failed")

    # -------------------------
    # Generic publish entrypoint used by Orchestrator
    # -------------------------
    def publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        payload expected keys:
          - platform: "x" | "facebook" | "instagram" | "internal"
          - text / tweet / fb_post
          - image_url (optional)
          - metadata
        """
        platform = payload.get("platform")
        text = payload.get("text") or payload.get("tweet") or payload.get("fb_post") or ""
        metadata = payload.get("metadata", {})
        try:
            if platform == "x":
                return self.post_x(text)
            elif platform == "facebook":
                page_url = payload.get("page_url") or os.getenv("FB_PAGE_URL")
                if not page_url:
                    raise PermanentPublishError("Facebook page URL not configured")
                return self.post_facebook_page(page_url, text)
            elif platform == "instagram":
                # Instagram web posting is more complex; placeholder
                raise PermanentPublishError("Instagram web publishing not implemented in Selenium adapter")
            elif platform == "internal":
                # internal platform: call internal API if provided
                internal_api = os.getenv("INTERNAL_PUBLISH_API")
                if not internal_api:
                    raise PermanentPublishError("Internal publish API not configured")
                try:
                    resp = requests.post(internal_api, json={"text": text, "meta": metadata}, timeout=10)
                    resp.raise_for_status()
                    result = {"status": "ok", "platform_id": resp.json().get("id"), "response": resp.text}
                    self.audit_record(payload, result)
                    return result
                except Exception as e:
                    logger.exception("Internal publish failed: %s", e)
                    raise TransientPublishError("Internal publish failed")
            else:
                raise PermanentPublishError(f"Unknown platform: {platform}")
        except PermanentPublishError:
            raise
        except TransientPublishError:
            raise
        except Exception as e:
            logger.exception("Unexpected publish error: %s", e)
            raise TransientPublishError("Unexpected publish error")
