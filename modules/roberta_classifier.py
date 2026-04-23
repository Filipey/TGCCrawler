# modules/roberta_classifier.py
"""
RoBERTa-based classifier for crypto vs. non-crypto content.

Two operating modes:
    1. REAL   loads your fine-tuned model from a local path or HuggingFace Hub.
    2. MOCK   simulates inference using keyword heuristics (no GPU/model needed).

Voting logic:
    A batch of message texts is classified individually. If the fraction of
    messages labelled as 'crypto' meets or exceeds ROBERTA_THRESHOLD, the
    entire chat is considered crypto-related.

Usage:
    from modules.roberta_classifier import build_classifier

    clf    = build_classifier(model_path="your-hf-user/roberta-crypto")
    result = clf.classify_batch(texts=["Bitcoin hits new ATH", "Good morning!"])

    if result.is_crypto:
        # proceed with full collection
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)


#  Result 

@dataclass
class ClassificationResult:
    """
    Output of a batch classification pass.

    Attributes:
        is_crypto:    True if the chat is crypto-related.
        score:        Mean crypto probability across all messages (0.0–1.0).
        n_messages:   Total messages evaluated.
        n_crypto:     Messages classified as crypto.
        label_counts: Raw label distribution {"crypto": N, "other": M}.
    """
    is_crypto:    bool
    score:        float
    n_messages:   int
    n_crypto:     int
    label_counts: dict


#  Protocol 

class CryptoClassifier(Protocol):
    def classify_batch(self, texts: list[str]) -> ClassificationResult: ...


#  Mock 

_CRYPTO_KEYWORDS = {
    "bitcoin", "btc", "ethereum", "eth", "crypto", "blockchain", "defi",
    "nft", "altcoin", "binance", "coinbase", "solana", "cardano", "ripple",
    "xrp", "dogecoin", "doge", "shiba", "token", "wallet", "hodl", "mining",
    "staking", "yield", "airdrop", "whitepaper", "satoshi", "web3", "dao",
    "liquidity", "decentralized", "exchange", "dex", "cex", "market cap",
    "bull", "bear", "pump", "dump", "ath", "dip", "portfolio", "memecoin",
}


class MockCryptoClassifier:
    """
    Simulated classifier for development and testing.
    Uses keyword density as a proxy for model inference.
    """

    def __init__(self, threshold: float = 0.5, noise: float = 0.1):
        self.threshold = threshold
        self.noise     = noise

    def _score_text(self, text: str) -> float:
        lower = text.lower()
        hits  = sum(1 for kw in _CRYPTO_KEYWORDS if kw in lower)
        base  = min(hits / max(len(text.split()) * 0.05, 1), 1.0)
        noise = random.gauss(0, self.noise)
        return float(min(max(base + noise, 0.0), 1.0))

    def classify_batch(self, texts: list[str]) -> ClassificationResult:
        if not texts:
            return ClassificationResult(False, 0.0, 0, 0, {})
        scores      = [self._score_text(t) for t in texts]
        crypto_flags = [s >= self.threshold for s in scores]
        n_crypto    = sum(crypto_flags)
        avg_score   = sum(scores) / len(scores)
        is_crypto   = (n_crypto / len(texts)) >= self.threshold
        logger.debug(
            f"[mock-roberta] {len(texts)} msgs → "
            f"n_crypto={n_crypto} avg={avg_score:.3f} is_crypto={is_crypto}"
        )
        return ClassificationResult(
            is_crypto    = is_crypto,
            score        = avg_score,
            n_messages   = len(texts),
            n_crypto     = n_crypto,
            label_counts = {"crypto": n_crypto, "other": len(texts) - n_crypto},
        )


#  Real (HuggingFace) 

class RoBERTaCryptoClassifier:
    """
    Wrapper around a fine-tuned RoBERTa text-classification model.

    Expects the model to output labels matching `crypto_label` for
    crypto-related content. Adapt `_map_label()` if your model uses
    numeric labels like LABEL_0 / LABEL_1.
    """

    def __init__(
        self,
        model_path:   str,
        crypto_label: int   = 1,
        threshold:    float = 0.5,
        batch_size:   int   = 32,
        use_gpu:      bool  = False,
    ):
        self.crypto_label = crypto_label
        self.threshold    = threshold
        self.batch_size   = batch_size

        try:
            import torch
            from transformers import pipeline as hf_pipeline
            device = 0 if (use_gpu and torch.cuda.is_available()) else -1
            logger.info(f"[roberta] Loading model from '{model_path}' on device={device}")
            self._pipe = hf_pipeline(
                "text-classification",
                model     = model_path,
                tokenizer = model_path,
                device    = device,
                truncation=True,
                max_length=512,
            )
            logger.info("[roberta] Model loaded successfully.")
        except ImportError as exc:
            raise RuntimeError(
                "transformers and/or torch not installed. "
                "Run: pip install transformers torch"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to load RoBERTa model: {exc}") from exc

    def _map_label(self, label: int) -> bool:
        return label == self.crypto_label

    def _chunk(self, texts: list[str]) -> list[list[str]]:
        return [texts[i: i + self.batch_size] for i in range(0, len(texts), self.batch_size)]

    def classify_batch(self, texts: list[str]) -> ClassificationResult:
        if not texts:
            return ClassificationResult(False, 0.0, 0, 0, {})

        all_results = []
        for chunk in self._chunk(texts):
            all_results.extend(self._pipe(chunk))

        crypto_scores: list[float] = []
        n_crypto      = 0
        label_counts: dict[int, int] = {}

        for res in all_results:
            label = res["label"]
            score = res["score"]
            label_counts[label] = label_counts.get(label, 0) + 1
            is_c         = self._map_label(label)
            crypto_prob  = score if is_c else 1.0 - score
            crypto_scores.append(crypto_prob)
            if crypto_prob >= self.threshold:
                n_crypto += 1

        avg_score = sum(crypto_scores) / len(crypto_scores)
        is_crypto = (n_crypto / len(texts)) >= self.threshold

        logger.info(
            f"[roberta] {len(texts)} msgs → "
            f"n_crypto={n_crypto} avg={avg_score:.3f} is_crypto={is_crypto}"
        )
        return ClassificationResult(
            is_crypto    = is_crypto,
            score        = avg_score,
            n_messages   = len(texts),
            n_crypto     = n_crypto,
            label_counts = label_counts,
        )


#  Factory 

def build_classifier(
    model_path: str | None = None,
    mock:       bool       = False,
    **kwargs,
) -> CryptoClassifier:
    """
    Builds the appropriate classifier.

    Args:
        model_path: Path to the fine-tuned model (local or HF Hub ID).
        mock:       If True, returns MockCryptoClassifier.
        **kwargs:   Forwarded to the chosen classifier constructor.
    """
    if mock or not model_path:
        logger.info("[classifier] Using MockCryptoClassifier")
        return MockCryptoClassifier(
            **{k: v for k, v in kwargs.items() if k in ("threshold", "noise")}
        )
    return RoBERTaCryptoClassifier(model_path=model_path, **kwargs)
