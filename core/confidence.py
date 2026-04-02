from __future__ import annotations

from config.config import MAX_SIGNAL_CONFIDENCE, SIGNAL_CONFIDENCE_CURVE_POWER


def clamp_confidence(value: float) -> float:
    return min(MAX_SIGNAL_CONFIDENCE, max(0.0, float(value or 0.0)))


def squash_confidence(raw_score: float) -> float:
    """
    Map a raw 0..1 score onto a steeper curve so top-end confidence becomes
    progressively harder to reach. A raw score near 1.0 is still required to
    approach MAX_SIGNAL_CONFIDENCE.
    """
    raw = min(1.0, max(0.0, float(raw_score or 0.0)))
    curved = raw ** SIGNAL_CONFIDENCE_CURVE_POWER
    return clamp_confidence(curved * MAX_SIGNAL_CONFIDENCE)


def boost_confidence(current: float, delta: float) -> float:
    """
    Apply diminishing-return boosts as the score approaches the configured cap.
    Early alignment matters; late boosts near the top barely move the needle.
    """
    current_score = clamp_confidence(current)
    delta_score = max(0.0, float(delta or 0.0))
    room = MAX_SIGNAL_CONFIDENCE - current_score
    if room <= 0.0 or delta_score <= 0.0:
        return current_score
    effective_delta = delta_score * (room / MAX_SIGNAL_CONFIDENCE)
    return clamp_confidence(current_score + effective_delta)
