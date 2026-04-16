import re
import unicodedata


_SUSPICIOUS_FRAGMENTS = (
    "‡",
    "§",
    "Â",
    "Ã",
    "à¤",
    "à¥",
    "à¦",
    "à§",
    "ðŸ",
    "â€™",
    "â€œ",
    "â€",
)

_ENCODING_PAIRS = (
    ("latin1", "utf-8"),
    ("cp1252", "utf-8"),
    ("mac_roman", "utf-8"),
    ("latin1", "cp1252"),
    ("cp1252", "latin1"),
)


def _cleanup_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    text = text.replace("\xa0", " ")
    return text.strip()


def _looks_suspicious(text: str) -> bool:
    if not text:
        return False
    return any(fragment in text for fragment in _SUSPICIOUS_FRAGMENTS)


def _is_indic_char(ch: str) -> bool:
    code = ord(ch)
    return (
        0x0900 <= code <= 0x097F or
        0x0980 <= code <= 0x09FF or
        0x0A00 <= code <= 0x0D7F
    )


def _score_text(text: str) -> float:
    score = 0.0
    for ch in text:
        if _is_indic_char(ch):
            score += 4.0
            continue

        category = unicodedata.category(ch)
        if ch.isascii() and (ch.isalnum() or ch.isspace() or ch in ".,:;!?()[]{}'\"-_/|@#%&*+=<>"):
            score += 0.2
        elif ch.isspace():
            score += 0.1
        elif category.startswith("L"):
            score += 0.5
        elif category.startswith("P"):
            score += 0.1
        elif category.startswith("C"):
            score -= 1.5

    if _looks_suspicious(text):
        score -= 12.0

    return score


def _candidate_repairs(text: str) -> list[str]:
    candidates = [text]

    for source_encoding, target_encoding in _ENCODING_PAIRS:
        try:
            repaired = text.encode(source_encoding).decode(target_encoding)
            candidates.append(repaired)
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    # Common two-step repair for text that was decoded twice.
    for source_encoding, mid_encoding in (("mac_roman", "latin1"), ("mac_roman", "cp1252")):
        try:
            repaired = text.encode(source_encoding).decode(mid_encoding).encode("latin1").decode("utf-8")
            candidates.append(repaired)
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    # Some copy/paste paths drop the first MacRoman marker even though the rest
    # of the mojibake pattern is intact.
    if text.startswith(("§", "•")) and "‡" in text[:12]:
        try:
            candidates.append(("‡" + text).encode("mac_roman").decode("utf-8"))
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

    deduped = []
    seen = set()
    for candidate in candidates:
        cleaned = _cleanup_text(candidate)
        if cleaned not in seen:
            seen.add(cleaned)
            deduped.append(cleaned)
    return deduped


def normalize_text(value):
    if not isinstance(value, str):
        return value

    text = _cleanup_text(value)
    if not text:
        return text

    candidates = _candidate_repairs(text)
    best = max(candidates, key=_score_text)

    # Keep the original unless the repair is clearly better.
    if _score_text(best) >= _score_text(text) + 4.0:
        return best

    return text
