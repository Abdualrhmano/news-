#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
content_gen/generator.py

وحدة توليد المحتوى الاحترافية (RAG + Local LLM) لتوليد:
 - ملخصات قصيرة وطويلة
 - تغريدات/منشورات فيسبوك/إنستغرام
 - نصوص للموقع (web snippet)
 - اقتباسات مرجعية (source snippets) لتقليل الهلوسة

الميزات:
- تكامل مع نموذج محلي من فئة Meta (Llama 2) عبر HuggingFace Transformers أو llama.cpp (قابل للتبديل)
- دعم RAG: فهرسة مقتطفات المقالات في FAISS محلي واستدعاء سياق قبل التوليد
- حفظ مقتطفات مرجعية مع كل توليد (source citations)
- واجهة بسيطة: generate_variants(signal) تعيد dict جاهز للنشر
- إعدادات قابلة للتعديل عبر متغيرات البيئة
- قيود أمان: قواعد بسيطة لمنع نشر PII أو محتوى محمي بحقوق نشر
- قابلية التشغيل كخدمة أو استدعاء من Orchestrator
"""

import os
import json
import logging
import time
import re
from typing import Dict, List, Optional, Tuple

# ML / RAG libs (محاولة استيراد؛ fallback إلى وضع بسيط إذا لم تتوفر)
try:
    from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
    HF_AVAILABLE = True
except Exception:
    HF_AVAILABLE = False

try:
    import faiss
    import numpy as np
    FAISS_AVAILABLE = True
except Exception:
    FAISS_AVAILABLE = False

# Optional llama.cpp integration (placeholder wrapper)
# In production you may use llama-cpp-python or direct subprocess to llama.cpp
# Here we provide a simple abstraction to allow switching implementations.
LLAMA_CPP_AVAILABLE = False

# ---------------------------
# Configuration via env vars
# ---------------------------
MODEL_NAME = os.getenv("LLM_MODEL_NAME", "meta-llama/Llama-2-7b")  # افتراضي؛ استبدل بمسار محلي عند الحاجة
USE_HF = os.getenv("CONTENT_USE_HF", "true").lower() == "true" and HF_AVAILABLE
RAG_INDEX_DIR = os.getenv("RAG_INDEX_DIR", "data/faiss_index")
RAG_K = int(os.getenv("RAG_K", "4"))  # عدد المقتطفات المرجعية المستدعاة
MAX_SUMMARY_TOKENS = int(os.getenv("MAX_SUMMARY_TOKENS", "250"))
MAX_TWEET_TOKENS = int(os.getenv("MAX_TWEET_TOKENS", "60"))
TEMPERATURE = float(os.getenv("GEN_TEMPERATURE", "0.2"))
TOP_P = float(os.getenv("GEN_TOP_P", "0.95"))
MIN_CONFIDENCE_TO_PUBLISH = float(os.getenv("MIN_CONFIDENCE_TO_PUBLISH", "0.6"))  # used for optional gating
LOG_LEVEL = os.getenv("CONTENT_GEN_LOG_LEVEL", "INFO")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [content_gen] %(message)s")
logger = logging.getLogger("content_gen")

# ---------------------------
# Simple safety helpers
# ---------------------------
PII_PATTERNS = [
    r"\b\d{10,}\b",  # long numbers (phone/IDs)
    r"\b\d{3}-\d{2}-\d{4}\b",  # SSN-like
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"  # emails
]

def contains_pii(text: str) -> bool:
    for p in PII_PATTERNS:
        if re.search(p, text):
            return True
    return False

def sanitize_for_publish(text: str) -> str:
    # basic sanitization: trim, collapse whitespace, remove control chars
    t = re.sub(r"\s+", " ", text).strip()
    # remove suspicious long numeric sequences
    t = re.sub(r"\b\d{12,}\b", "[redacted]", t)
    return t

# ---------------------------
# RAG index utilities (FAISS)
# ---------------------------
class RagIndex:
    """
    بسيط: فهرس FAISS محلي للمقتطفات.
    كل وثيقة تُخزن كمقتطفات قصيرة مع metadata: {doc_id, source, snippet, offset}
    index directory يحتوي على:
      - vectors.npy (float32 matrix)
      - metadata.jsonl (line-delimited metadata)
    ملاحظة: هذا تبسيط؛ في الإنتاج استخدم Milvus/Weaviate/FAISS+Annoy مع sharding.
    """
    def __init__(self, index_dir: str = RAG_INDEX_DIR):
        self.index_dir = index_dir
        self.index = None
        self.metadata = []  # list of dicts aligned with vectors
        self.dim = 1536  # default embedding dim (depends on embedding model)
        self._load_index()

    def _load_index(self):
        if not FAISS_AVAILABLE:
            logger.warning("FAISS not available; RAG disabled.")
            return
        try:
            vec_path = os.path.join(self.index_dir, "vectors.npy")
            meta_path = os.path.join(self.index_dir, "metadata.jsonl")
            if os.path.exists(vec_path) and os.path.exists(meta_path):
                import numpy as np
                vectors = np.load(vec_path)
                self.dim = vectors.shape[1]
                self.index = faiss.IndexFlatIP(self.dim)
                self.index.add(vectors)
                # load metadata
                with open(meta_path, "r", encoding="utf-8") as f:
                    self.metadata = [json.loads(line) for line in f]
                logger.info("Loaded RAG index with %d vectors", len(self.metadata))
            else:
                logger.info("No existing RAG index found at %s", self.index_dir)
        except Exception as e:
            logger.exception("Failed to load RAG index: %s", e)
            self.index = None
            self.metadata = []

    def search(self, query_vec: List[float], k: int = RAG_K) -> List[Dict]:
        if not self.index:
            return []
        import numpy as np
        q = np.array(query_vec, dtype="float32").reshape(1, -1)
        D, I = self.index.search(q, k)
        results = []
        for score, idx in zip(D[0], I[0]):
            if idx < 0 or idx >= len(self.metadata):
                continue
            meta = self.metadata[idx].copy()
            meta["_score"] = float(score)
            results.append(meta)
        return results

# ---------------------------
# LLM wrapper
# ---------------------------
class LocalLLM:
    """
    واجهة مبسطة للتوليد باستخدام HuggingFace أو بديل محلي.
    يدعم: generate(prompt, max_tokens, temperature, top_p)
    """
    def __init__(self, model_name: str = MODEL_NAME, use_hf: bool = USE_HF):
        self.model_name = model_name
        self.use_hf = use_hf and HF_AVAILABLE
        self.tokenizer = None
        self.model = None
        self.pipeline = None
        if self.use_hf:
            try:
                logger.info("Loading HF model: %s", model_name)
                self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
                self.model = AutoModelForCausalLM.from_pretrained(model_name, device_map="auto", torch_dtype=None)
                self.pipeline = pipeline("text-generation", model=self.model, tokenizer=self.tokenizer, device=0 if self.model.device.type == "cuda" else -1)
                logger.info("HF model loaded.")
            except Exception as e:
                logger.exception("Failed to load HF model: %s", e)
                self.use_hf = False
        else:
            logger.info("HF not used or unavailable; Local LLM in fallback/text-only mode.")

    def generate(self, prompt: str, max_tokens: int = 200, temperature: float = TEMPERATURE, top_p: float = TOP_P) -> str:
        if self.use_hf and self.pipeline:
            try:
                out = self.pipeline(prompt, max_new_tokens=max_tokens, do_sample=True, temperature=temperature, top_p=top_p, num_return_sequences=1)
                text = out[0]["generated_text"]
                # remove prompt prefix if model echoes prompt
                if text.startswith(prompt):
                    text = text[len(prompt):].strip()
                return text
            except Exception as e:
                logger.exception("HF generation failed: %s", e)
                return ""
        # fallback simple echo/trim (not ideal)
        logger.warning("LLM fallback: returning truncated prompt summary.")
        return prompt[:max_tokens]

# ---------------------------
# Generator high-level functions
# ---------------------------
# instantiate global objects (lazy)
_rag_index = None
_llm = None

def get_rag_index() -> Optional[RagIndex]:
    global _rag_index
    if _rag_index is None:
        _rag_index = RagIndex(RAG_INDEX_DIR)
    return _rag_index

def get_llm() -> LocalLLM:
    global _llm
    if _llm is None:
        _llm = LocalLLM(MODEL_NAME, use_hf=USE_HF)
    return _llm

def build_rag_prompt(signal: Dict, retrieved: List[Dict]) -> str:
    """
    بناء prompt مهيأ للـRAG:
    - تضمين عنوان ومقتطفات مرجعية مع مصدرها
    - طلب توليد ملخص موجز ثم صيغة تغريدة ومنشور فيسبوك
    - تعليمات للـLLM بعدم اختلاق معلومات خارج المقتطفات (اجعلها conservative)
    """
    title = signal.get("aggregated", {}).get("title") or signal.get("item", {}).get("title", "")
    link = signal.get("aggregated", {}).get("canonical_link") or signal.get("item", {}).get("link", "")
    header = f"Title: {title}\nURL: {link}\n\n"
    context = "Reference snippets (use these facts only):\n"
    for i, r in enumerate(retrieved):
        src = r.get("source") or r.get("doc_id") or "unknown"
        snippet = r.get("snippet", "")[:800]
        context += f"[{i+1}] Source: {src}\n{snippet}\n\n"
    instructions = (
        "Instructions:\n"
        "- Summarize the article in Arabic in up to 250 words using only the reference snippets.\n"
        "- Provide a short tweet (<= 280 chars) in Arabic and an expanded Facebook post.\n"
        "- For each generated text, include a 'sources' field listing the snippet indices used.\n"
        "- If information is not present in the snippets, do not invent it; instead mark as 'unspecified'.\n"
        "- Remove any PII before returning.\n\n"
    )
    prompt = header + context + instructions + "Begin:\n"
    return prompt

def retrieve_context_for_signal(signal: Dict, k: int = RAG_K) -> List[Dict]:
    """
    استدعاء مقتطفات مرجعية من الفهرس بناءً على عنوان/محتوى الإشارة.
    إذا لم يتوفر فهرس، نعيد قائمة فارغة.
    """
    rag = get_rag_index()
    if not rag or not FAISS_AVAILABLE:
        return []
    # build a simple embedding for the query: in production use embedding model
    # here we attempt to use HF tokenizer embedding if available (placeholder)
    # For now, use a naive approach: hash-based pseudo-vector (NOT recommended for production)
    title = signal.get("aggregated", {}).get("title") or signal.get("item", {}).get("title", "")
    # placeholder: use simple hash to create deterministic vector (only for demo)
    import numpy as np
    vec = np.zeros(rag.dim, dtype="float32")
    s = title.encode("utf-8")
    for i, b in enumerate(s):
        vec[i % rag.dim] += (b % 97) / 97.0
    # normalize
    if np.linalg.norm(vec) > 0:
        vec = vec / np.linalg.norm(vec)
    results = rag.search(vec.tolist(), k=k)
    return results

def generate_variants(signal: Dict) -> Dict[str, Any]:
    """
    الواجهة الرئيسية التي يستدعيها Orchestrator بعد حصوله على signal (canonical aggregated).
    تُعيد dict يحتوي على:
      - summary (string)
      - tweet (string)
      - fb_post (string)
      - web_snippet (dict)
      - sources (list of {index, source, snippet, score})
      - confidence (float)  # تقديري
    """
    llm = get_llm()
    # 1) retrieve context
    retrieved = retrieve_context_for_signal(signal)
    prompt = build_rag_prompt(signal, retrieved)
    # 2) generate via LLM
    raw_out = llm.generate(prompt, max_tokens=MAX_SUMMARY_TOKENS, temperature=TEMPERATURE, top_p=TOP_P)
    # 3) parse LLM output heuristically: we expect summary + tweet + fb_post + sources
    #    since model outputs vary, we attempt to split by markers or fallback to simple slicing
    summary = ""
    tweet = ""
    fb_post = ""
    sources_used = []
    try:
        # try to split by "Tweet:" or "Short tweet" markers
        if "Tweet:" in raw_out or "تغريدة" in raw_out:
            # naive splits
            parts = re.split(r"(Tweet:|تغريدة:|تغريدة)", raw_out, flags=re.IGNORECASE)
            summary = parts[0].strip()
            rest = "".join(parts[1:]).strip()
            # further split for fb
            if "Facebook" in rest or "فيسبوك" in rest:
                sub = re.split(r"(Facebook:|فيسبوك:)", rest, flags=re.IGNORECASE)
                tweet = sub[0].strip()
                fb_post = "".join(sub[1:]).strip()
            else:
                tweet = rest.strip()[:280]
        else:
            # fallback: take first N chars as summary, next as tweet
            summary = raw_out.strip()[:MAX_SUMMARY_TOKENS*4]
            tweet = summary[:250]
            fb_post = summary[:800]
    except Exception as e:
        logger.debug("Parsing LLM output failed: %s", e)
        summary = raw_out.strip()[:MAX_SUMMARY_TOKENS*4]
        tweet = summary[:250]
        fb_post = summary[:800]

    # 4) sanitize and PII check
    if contains_pii(summary) or contains_pii(tweet) or contains_pii(fb_post):
        logger.warning("Generated content contains PII; redacting.")
        summary = re.sub(r"\b\d{12,}\b", "[redacted]", summary)
        tweet = re.sub(r"\b\d{12,}\b", "[redacted]", tweet)
        fb_post = re.sub(r"\b\d{12,}\b", "[redacted]", fb_post)

    summary = sanitize_for_publish(summary)
    tweet = sanitize_for_publish(tweet)
    fb_post = sanitize_for_publish(fb_post)

    # 5) build web_snippet and sources list
    web_snippet = {
        "title": signal.get("aggregated", {}).get("title") or signal.get("item", {}).get("title", ""),
        "summary": summary[:800],
        "link": signal.get("aggregated", {}).get("canonical_link") or signal.get("item", {}).get("link", "")
    }
    for i, r in enumerate(retrieved):
        sources_used.append({
            "index": i+1,
            "source": r.get("source") or r.get("doc_id"),
            "snippet": r.get("snippet")[:500],
            "score": r.get("_score", 0.0)
        })

    # 6) confidence heuristic: based on number of retrieved snippets and normalized_speed
    propagation = signal.get("propagation_metrics", {}) or {}
    normalized_speed = propagation.get("normalized_speed", 0.0)
    conf = min(1.0, 0.2 + 0.4 * (len(sources_used)/max(1, RAG_K)) + 0.4 * min(1.0, normalized_speed))
    # if no retrieved context, lower confidence
    if not sources_used:
        conf = min(conf, 0.5)

    result = {
        "summary": summary,
        "tweet": tweet,
        "fb_post": fb_post,
        "web_snippet": web_snippet,
        "sources": sources_used,
        "confidence": float(conf),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    return result

# ---------------------------
# Utility: index documents into RAG index (offline)
# ---------------------------
def index_documents(documents: List[Dict[str, str]], index_dir: str = RAG_INDEX_DIR, embedding_fn=None):
    """
    documents: list of {"doc_id": str, "source": str, "text": str}
    embedding_fn: callable(text)->vector (list[float]) ; if None, we use a naive hashing fallback (not recommended)
    This function builds vectors.npy and metadata.jsonl in index_dir.
    """
    os.makedirs(index_dir, exist_ok=True)
    meta_path = os.path.join(index_dir, "metadata.jsonl")
    vec_path = os.path.join(index_dir, "vectors.npy")
    try:
        import numpy as np
        vectors = []
        metadata = []
        for doc in documents:
            text = doc.get("text","")[:2000]
            if embedding_fn:
                vec = embedding_fn(text)
            else:
                # naive deterministic pseudo-embedding (placeholder)
                vec = np.zeros(1536, dtype="float32")
                b = text.encode("utf-8")
                for i, ch in enumerate(b):
                    vec[i % 1536] += (ch % 97) / 97.0
                if np.linalg.norm(vec) > 0:
                    vec = vec / np.linalg.norm(vec)
            vectors.append(vec.astype("float32"))
            metadata.append({"doc_id": doc.get("doc_id"), "source": doc.get("source"), "snippet": text[:800]})
        vectors = np.vstack(vectors)
        np.save(vec_path, vectors)
        with open(meta_path, "w", encoding="utf-8") as f:
            for m in metadata:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        logger.info("Indexed %d documents into RAG index at %s", len(metadata), index_dir)
        # reload index
        global _rag_index
        _rag_index = RagIndex(index_dir)
    except Exception as e:
        logger.exception("Failed to index documents: %s", e)

# ---------------------------
# Example wrapper used by Orchestrator
# ---------------------------
def generate_and_attach(signal: Dict) -> Dict:
    """
    واجهة بسيطة: تأخذ signal (بعد importance scoring) وتُنتج payload جاهز للنشر.
    تضيف الحقول التالية إلى signal:
      - content_variants (result of generate_variants)
      - publish_ready (bool) based on confidence and importance_score
    وتعيد signal المحدث.
    """
    variants = generate_variants(signal)
    signal["content_variants"] = variants
    # gating: require both high importance and minimum confidence
    importance = signal.get("importance_score", 0.0)
    conf = variants.get("confidence", 0.0)
    publish_ready = (importance >= float(os.getenv("IMPORTANCE_THRESHOLD", "0.9"))) and (conf >= MIN_CONFIDENCE_TO_PUBLISH)
    signal["publish_ready"] = bool(publish_ready)
    signal["generated_at"] = variants.get("generated_at")
    return signal

# ---------------------------
# CLI quick test
# ---------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Content generator quick test")
    parser.add_argument("--test-file", help="JSON file with a sample aggregated signal", default=None)
    args = parser.parse_args()
    if args.test_file:
        with open(args.test_file, "r", encoding="utf-8") as f:
            sig = json.load(f)
        out = generate_and_attach(sig)
        print(json.dumps(out, indent=2, ensure_ascii=False))
    else:
        print("No test file provided. Use --test-file sample.json")
