#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
models/importance_scorer.py

وحدة احترافية لحساب أهمية/أثر الخبر (importance scoring).
الوظائف الرئيسية:
- تحويل السجلات المجمعة (canonical aggregated) وميزات المقاييس والمصداقية إلى ميزات رقمية
- واجهة للتنبؤ بالـimportance_score (0..1)
- دعم التدريب المبدئي (offline) وإعادة التدريب الدورية وpartial_fit للتعلم المستمر
- نشر النتائج إلى Kafka topic: tech.signals (أو topic مخصص)
- حفظ/تحميل artifacts للنموذج (joblib)
- explainability: إرجاع أهم الميزات المساهمة في القرار (SHAP أو وزن بسيط إذا لم يتوفر SHAP)

تصميم:
- افتراضيًا يستخدم LogisticRegression (sklearn) كنموذج baseline خفيف.
- يمكن استبداله بـLightGBM أو XGBoost لاحقًا عند الحاجة.
- يدعم واجهة بسيطة للتكامل مع Orchestrator وAgent Analyzer.
"""

from typing import Dict, Any, List, Tuple, Optional
import os
import json
import time
import logging
import math

# ML libs
try:
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    import joblib
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# Optional: lightgbm fallback
try:
    import lightgbm as lgb
    LGB_AVAILABLE = True
except Exception:
    LGB_AVAILABLE = False

from confluent_kafka import Producer

# ---------------------------
# Configuration via env vars
# ---------------------------
MODEL_PATH = os.getenv("IMPORTANCE_MODEL_PATH", "models/importance_model.joblib")
SCALER_PATH = os.getenv("IMPORTANCE_SCALER_PATH", "models/importance_scaler.joblib")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SIGNAL_TOPIC = os.getenv("SIGNAL_TOPIC", "tech.signals")
LOG_LEVEL = os.getenv("IMPORTANCE_LOG_LEVEL", "INFO")
DEFAULT_THRESHOLD = float(os.getenv("IMPORTANCE_THRESHOLD", "0.9"))  # threshold for high-impact

# Feature weights fallback (if no model available)
FALLBACK_WEIGHTS = {
    "log_total_views": 0.35,
    "log_max_likes": 0.15,
    "normalized_speed": 0.25,
    "source_credibility": 0.15,
    "entity_prominence": 0.10
}

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [importance_scorer] %(message)s")
logger = logging.getLogger("importance_scorer")

# ---------------------------
# Kafka producer
# ---------------------------
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# ---------------------------
# Model holder
# ---------------------------
_model = None
_scaler = None
_model_type = None  # "sklearn" or "lgb" or "fallback"

# ---------------------------
# Feature engineering
# ---------------------------
def safe_log(x: Optional[float]) -> float:
    try:
        if x is None:
            return 0.0
        return math.log(float(x) + 1.0)
    except Exception:
        return 0.0

def compute_entity_prominence(aggregated: Dict[str, Any]) -> float:
    """
    بسيط: عدد الكيانات المهمة المذكورة في العناوين/المحتوى مقسوم على sqrt(number of mentions)
    aggregated: canonical aggregated record
    """
    mentions = aggregated.get("mentions", []) if aggregated else []
    # assume each mention may have meta.entities list (populated by NER)
    total_entities = 0
    for m in mentions:
        ents = m.get("meta", {}).get("entities", []) if m.get("meta") else []
        total_entities += len(ents)
    denom = math.sqrt(max(1, len(mentions)))
    return float(total_entities) / denom

def compute_features_from_aggregated(aggregated: Dict[str, Any], propagation_metrics: Optional[Dict[str, Any]] = None, credibility: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    """
    تحويل سجل مجمّع إلى ميزات رقمية جاهزة للنموذج.
    الميزات المقترحة:
      - log_total_views
      - log_max_likes
      - mention_count
      - normalized_speed (من propagation_metrics)
      - growth_rate
      - source_credibility (0..1)
      - entity_prominence
      - recency_hours (من last_seen)
    """
    features = {}
    agg = aggregated or {}
    mentions = agg.get("mentions", []) if agg else []
    total_views = agg.get("total_views") or 0
    max_likes = agg.get("max_likes") or 0
    mention_count = agg.get("mention_count") or len(mentions)
    first_seen = agg.get("first_seen")
    last_seen = agg.get("last_seen")

    features["log_total_views"] = safe_log(total_views)
    features["log_max_likes"] = safe_log(max_likes)
    features["mention_count"] = float(mention_count)

    # propagation metrics
    if propagation_metrics:
        features["normalized_speed"] = float(propagation_metrics.get("normalized_speed", 0.0))
        features["growth_rate"] = float(propagation_metrics.get("growth_rate", 0.0))
        features["doubling_time_hours"] = float(propagation_metrics.get("doubling_time_hours") or 0.0)
    else:
        features["normalized_speed"] = 0.0
        features["growth_rate"] = 0.0
        features["doubling_time_hours"] = 0.0

    # credibility
    if credibility:
        features["source_credibility"] = float(credibility.get("score", 0.5))
    else:
        features["source_credibility"] = 0.5  # neutral

    # entity prominence
    features["entity_prominence"] = float(compute_entity_prominence(agg))

    # recency: hours since first_seen
    try:
        if last_seen:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
            hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
            features["hours_since_last_seen"] = float(hours)
        else:
            features["hours_since_last_seen"] = 9999.0
    except Exception:
        features["hours_since_last_seen"] = 9999.0

    return features

# ---------------------------
# Model utilities: load/save/train
# ---------------------------
def load_model(path: str = MODEL_PATH):
    global _model, _scaler, _model_type
    if not SKLEARN_AVAILABLE:
        logger.warning("sklearn not available; using fallback weights.")
        _model = None
        _scaler = None
        _model_type = "fallback"
        return
    if os.path.exists(path):
        try:
            obj = joblib.load(path)
            if isinstance(obj, dict) and "model" in obj and "scaler" in obj:
                _model = obj["model"]
                _scaler = obj["scaler"]
                _model_type = "sklearn"
                logger.info("Loaded importance model from %s", path)
                return
            # backward compat: direct pipeline
            _model = obj
            _scaler = None
            _model_type = "sklearn"
            logger.info("Loaded importance model (pipeline) from %s", path)
            return
        except Exception as e:
            logger.exception("Failed to load model: %s", e)
    # no model found -> fallback
    logger.info("No model artifact found at %s. Using fallback scoring.", path)
    _model = None
    _scaler = None
    _model_type = "fallback"

def save_model(model_obj, scaler_obj=None, path: str = MODEL_PATH):
    try:
        payload = {"model": model_obj, "scaler": scaler_obj}
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump(payload, path)
        logger.info("Saved model artifact to %s", path)
    except Exception as e:
        logger.exception("Failed to save model: %s", e)

def train_offline(X: List[Dict[str, float]], y: List[float], model_path: str = MODEL_PATH):
    """
    تدريب مبدئي offline. X: list of feature dicts, y: list of labels (0/1).
    يحول X إلى DataFrame، يبني scaler + LogisticRegression pipeline، ويحفظ النموذج.
    """
    global _model, _scaler, _model_type
    if not SKLEARN_AVAILABLE:
        raise RuntimeError("sklearn not available in environment")
    df = pd.DataFrame(X)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(df.values)
    clf = LogisticRegression(max_iter=1000)
    clf.fit(Xs, y)
    _model = clf
    _scaler = scaler
    _model_type = "sklearn"
    save_model(_model, _scaler, model_path)
    logger.info("Trained offline importance model on %d samples", len(y))
    return _model

def partial_update(X: List[Dict[str, float]], y: List[float]):
    """
    تحديث أونلاين إن كان النموذج يدعم partial_fit (مثل SGDClassifier).
    هنا كـplaceholder: سنعيد تدريب صغير إذا لم يتوفر partial_fit.
    """
    global _model, _scaler, _model_type
    if _model_type != "sklearn" or _model is None:
        logger.info("No sklearn model to partial_update; performing offline retrain fallback.")
        return train_offline(X, y)
    # fallback: retrain on combined small dataset (in production use incremental learners)
    try:
        df = pd.DataFrame(X)
        Xs = _scaler.transform(df.values) if _scaler else df.values
        _model.fit(Xs, y)
        save_model(_model, _scaler, MODEL_PATH)
        logger.info("Performed partial update (retrained) on %d samples", len(y))
    except Exception as e:
        logger.exception("Partial update failed: %s", e)

# ---------------------------
# Prediction & explainability
# ---------------------------
def predict_score_from_features(features: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
    """
    إرجاع (score, explain_dict)
    explain_dict: مساهمة كل ميزة (بسيطة: وزن * feature) أو SHAP values إن توفرت
    """
    global _model, _scaler, _model_type
    feat_names = list(features.keys())
    X_vec = None
    try:
        if _model_type == "sklearn" and _model is not None:
            import numpy as _np
            df = _np.array([features[k] for k in feat_names], dtype=float).reshape(1, -1)
            if _scaler:
                df = _scaler.transform(df)
            prob = float(_model.predict_proba(df)[0, 1])
            # explain: use coef * x if logistic regression
            explain = {}
            try:
                coefs = _model.coef_[0]
                for i, k in enumerate(feat_names):
                    explain[k] = float(coefs[i] * (df[0, i] if hasattr(df, "__getitem__") else df[0][i]))
            except Exception:
                for k in feat_names:
                    explain[k] = 0.0
            return prob, explain
        else:
            # fallback weighted sum normalized to 0..1
            score = 0.0
            total_w = sum(FALLBACK_WEIGHTS.values())
            explain = {}
            for k, w in FALLBACK_WEIGHTS.items():
                v = features.get(k, 0.0)
                contrib = w * v
                explain[k] = contrib
                score += contrib
            # normalize by total_w and apply sigmoid-like scaling
            score = score / (total_w + 1e-9)
            score = 1.0 / (1.0 + math.exp(- (score - 0.5) * 3.0))  # sharpen
            return float(score), explain
    except Exception as e:
        logger.exception("Prediction failed: %s", e)
        # fallback neutral
        return 0.0, {k: 0.0 for k in feat_names}

def predict_from_aggregated(aggregated: Dict[str, Any], propagation_metrics: Optional[Dict[str, Any]] = None, credibility: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    واجهة رئيسية: تأخذ aggregated record وتعيد signal dict:
      {
        "canonical_id": ...,
        "importance_score": 0..1,
        "features": {...},
        "explain": {...},
        "is_high_impact": bool (thresholded)
      }
    """
    features = compute_features_from_aggregated(aggregated, propagation_metrics, credibility)
    score, explain = predict_score_from_features(features)
    is_high = score >= DEFAULT_THRESHOLD
    signal = {
        "canonical_id": aggregated.get("canonical_id") or aggregated.get("canonical_link"),
        "importance_score": float(score),
        "features": features,
        "explain": explain,
        "is_high_impact": bool(is_high),
        "computed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    return signal

# ---------------------------
# Publish signal to Kafka
# ---------------------------
def publish_signal(signal: Dict[str, Any], topic: str = SIGNAL_TOPIC):
    try:
        producer.produce(topic, json.dumps(signal, ensure_ascii=False).encode("utf-8"))
        producer.poll(0)
        logger.info("Published signal for %s -> score=%.3f high=%s", signal.get("canonical_id"), signal.get("importance_score"), signal.get("is_high_impact"))
    except Exception as e:
        logger.exception("Failed to publish signal: %s", e)

# ---------------------------
# Initialization: load model if exists
# ---------------------------
load_model(MODEL_PATH)

# ---------------------------
# CLI quick test / training helper
# ---------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Importance scorer quick utilities")
    parser.add_argument("--train", help="Train on JSONL file with records [{'features':..., 'label':0/1}]", default=None)
    parser.add_argument("--predict", help="Predict on aggregated JSON file", default=None)
    args = parser.parse_args()

    if args.train:
        # load JSONL
        X = []
        y = []
        with open(args.train, "r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                feats = rec.get("features")
                label = rec.get("label", 0)
                X.append(feats)
                y.append(label)
        train_offline(X, y)
    elif args.predict:
        with open(args.predict, "r", encoding="utf-8") as f:
            agg = json.load(f)
        sig = predict_from_aggregated(agg)
        print(json.dumps(sig, indent=2, ensure_ascii=False))
    else:
        print("No action specified. Use --train or --predict.")
