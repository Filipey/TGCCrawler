# modules/language_detector.py
"""
Language detection module for collected Telegram messages.

Purpose:
    After collecting messages from a channel or group, this module checks
    whether the content is predominantly in English. Non-English chats are
    flagged and their pipeline status is set to 'discarded_language' so they
    are permanently skipped by the main collector loop.

Design decisions:
    - Uses langdetect (Naive Bayes + character n-grams, 55 languages, 99.77%
      accuracy on news corpora) as primary detector — same library used by the
      TeleScope paper (Gangopadhyay et al., ICWSM 2025).
    - Falls back to langid as a secondary detector when langdetect is
      non-deterministic or throws an exception on short texts.
    - Operates on a combined corpus of all message texts from the chat
      (concatenated with newlines) to get a stable language signal, rather
      than classifying each message individually — again following the
      methodology described in TeleScope Section 4.1.
    - A chat is considered English if the fraction of messages individually
      detected as English meets or exceeds ENGLISH_THRESHOLD (configurable).

Usage:
    from modules.language_detector import LanguageDetector

    detector = LanguageDetector()
    result   = detector.detect(messages=[...])

    if result.is_english:
        # proceed with RoBERTa classification
    else:
        # mark chat as discarded_language
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Minimum fraction of messages detected as English
# to classify the entire chat as English-dominant
DEFAULT_ENGLISH_THRESHOLD: float = 0.60

# Minimum number of characters required for reliable language detection
# (very short texts produce unreliable results)
MIN_CHARS_FOR_DETECTION: int = 20

# ISO 639-1 code for English
ENGLISH_LANG_CODE: str = "en"


#  Result dataclass 

@dataclass
class LanguageDetectionResult:
    """
    Outcome of language detection for a single chat's message batch.

    Attributes:
        is_english:         True if the chat is predominantly English.
        dominant_language:  Most frequent detected language code (e.g. "en").
        english_fraction:   Fraction of messages detected as English (0.0–1.0).
        n_messages_checked: Number of messages passed to the detector.
        n_too_short:        Messages skipped due to insufficient text length.
        language_counts:    Per-language message counts {"en": 42, "ru": 5, ...}.
        corpus_language:    Language detected on the full concatenated corpus.
    """
    is_english:          bool
    dominant_language:   Optional[str]
    english_fraction:    float
    n_messages_checked:  int
    n_too_short:         int
    language_counts:     dict[str, int]
    corpus_language:     Optional[str]


#  Detector 

class LanguageDetector:
    """
    Detects the dominant language of a collection of Telegram messages.

    Two detection strategies are combined:
        1. Per-message detection   used to compute english_fraction.
        2. Corpus detection        full concatenation of all texts.

    A chat is flagged as English when english_fraction >= threshold.

    Args:
        threshold:    Minimum English fraction to accept a chat (default 0.60).
        min_chars:    Minimum characters for a message to be evaluated.
        use_langid:   Enable langid as fallback when langdetect fails.
    """

    def __init__(
        self,
        threshold:   float = DEFAULT_ENGLISH_THRESHOLD,
        min_chars:   int   = MIN_CHARS_FOR_DETECTION,
        use_langid:  bool  = True,
    ):
        self.threshold  = threshold
        self.min_chars  = min_chars
        self.use_langid = use_langid

        self._langdetect_available = self._check_langdetect()
        self._langid_available     = self._check_langid() if use_langid else False

        if not self._langdetect_available and not self._langid_available:
            raise RuntimeError(
                "No language detection library available. "
                "Install at least one: pip install langdetect  OR  pip install langid"
            )

    #  Library availability checks 

    @staticmethod
    def _check_langdetect() -> bool:
        try:
            import langdetect  # noqa: F401
            return True
        except ImportError:
            logger.warning("[lang] langdetect not installed — pip install langdetect")
            return False

    @staticmethod
    def _check_langid() -> bool:
        try:
            import langid  # noqa: F401
            return True
        except ImportError:
            logger.debug("[lang] langid not installed (optional fallback)")
            return False

    #  Low-level detectors 

    def _detect_langdetect(self, text: str) -> Optional[str]:
        """Returns ISO 639-1 code or None on failure."""
        try:
            from langdetect import DetectorFactory, LangDetectException, detect
            DetectorFactory.seed = 42  # reproducibility
            return detect(text)
        except Exception:
            return None

    def _detect_langid(self, text: str) -> Optional[str]:
        """Returns ISO 639-1 code or None on failure."""
        try:
            import langid
            lang, _ = langid.classify(text)
            return lang
        except Exception:
            return None

    def _detect_single(self, text: str) -> Optional[str]:
        """
        Detects language for a single text string.
        Falls back from langdetect → langid → None.
        """
        if not text or len(text.strip()) < self.min_chars:
            return None

        # Strip URLs and Telegram mentions before detection
        # (they introduce noise in short messages)
        cleaned = re.sub(r"https?://\S+", "", text)
        cleaned = re.sub(r"@\w+", "", cleaned).strip()

        if len(cleaned) < self.min_chars:
            return None

        if self._langdetect_available:
            lang = self._detect_langdetect(cleaned)
            if lang:
                return lang

        if self._langid_available:
            return self._detect_langid(cleaned)

        return None

    #  Public API 

    def detect(self, messages: list) -> LanguageDetectionResult:
        """
        Detects the dominant language of a list of messages.

        Args:
            messages: List of CollectedMessage dataclass instances OR plain
                      strings. Both are accepted — the method extracts the
                      .text attribute if the item is not a string.

        Returns:
            LanguageDetectionResult with full breakdown.
        """
        # Normalise input to list of strings
        texts: list[str] = []
        for m in messages:
            if isinstance(m, str):
                texts.append(m)
            else:
                t = getattr(m, "text", "") or ""
                if t:
                    texts.append(t)

        if not texts:
            logger.warning("[lang] No texts provided for language detection.")
            return LanguageDetectionResult(
                is_english         = False,
                dominant_language  = None,
                english_fraction   = 0.0,
                n_messages_checked = 0,
                n_too_short        = 0,
                language_counts    = {},
                corpus_language    = None,
            )

        #  Per-message detection 
        lang_counts: dict[str, int] = {}
        n_too_short = 0

        for text in texts:
            if len(text.strip()) < self.min_chars:
                n_too_short += 1
                continue
            lang = self._detect_single(text)
            if lang:
                lang_counts[lang] = lang_counts.get(lang, 0) + 1
            else:
                n_too_short += 1

        n_checked      = len(texts) - n_too_short
        n_english      = lang_counts.get(ENGLISH_LANG_CODE, 0)
        english_frac   = n_english / n_checked if n_checked > 0 else 0.0
        dominant_lang  = max(lang_counts, key=lang_counts.get) if lang_counts else None

        #  Corpus-level detection (TeleScope methodology) 
        corpus = "\n".join(texts)
        corpus_lang = self._detect_single(corpus[:5000])  # cap at 5K chars

        is_english = english_frac >= self.threshold

        logger.info(
            f"[lang] {len(texts)} messages | "
            f"english_fraction={english_frac:.2%} | "
            f"dominant={dominant_lang!r} | "
            f"corpus_lang={corpus_lang!r} | "
            f"is_english={is_english}"
        )

        return LanguageDetectionResult(
            is_english         = is_english,
            dominant_language  = dominant_lang,
            english_fraction   = english_frac,
            n_messages_checked = n_checked,
            n_too_short        = n_too_short,
            language_counts    = lang_counts,
            corpus_language    = corpus_lang,
        )

    def detect_texts(self, texts: list[str]) -> LanguageDetectionResult:
        """Convenience wrapper that accepts plain strings instead of message objects."""
        return self.detect(texts)


#  Factory 

def build_language_detector(
    threshold:  float = DEFAULT_ENGLISH_THRESHOLD,
    min_chars:  int   = MIN_CHARS_FOR_DETECTION,
    use_langid: bool  = True,
) -> LanguageDetector:
    """
    Builds a LanguageDetector with the given configuration.
    Raises RuntimeError if no detection library is installed.
    """
    return LanguageDetector(
        threshold  = threshold,
        min_chars  = min_chars,
        use_langid = use_langid,
    )
