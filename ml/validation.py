from __future__ import annotations

from typing import Any, Callable, Dict

import numpy as np


def _accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if len(y_true) == 0:
        return 0.0
    return float(np.mean(y_true == y_pred))


def _baseline_accuracy(y_true: np.ndarray) -> float:
    if len(y_true) == 0:
        return 0.0
    positive_rate = float(np.mean(y_true))
    return max(positive_rate, 1.0 - positive_rate)


def evaluate_classifier_research(
    X: np.ndarray,
    y: np.ndarray,
    model_factory: Callable[[], Any],
    train_test_split: float,
    min_walk_forward_train: int = 120,
    walk_forward_window: int = 40,
    walk_forward_step: int = 20,
) -> Dict[str, Any]:
    """
    Compute lightweight research metadata for time-series classification models.

    The report intentionally stays simple and cheap enough for background
    training: one chronological holdout plus an expanding-window walk-forward.
    """
    report: Dict[str, Any] = {
        "validation_method": "time_split+walk_forward",
        "sample_count": int(len(X) if X is not None else 0),
        "class_balance": 0.0,
        "holdout_accuracy": 0.0,
        "holdout_baseline_accuracy": 0.0,
        "holdout_samples": 0,
        "walk_forward_accuracy": 0.0,
        "walk_forward_baseline_accuracy": 0.0,
        "walk_forward_edge": 0.0,
        "walk_forward_samples": 0,
        "walk_forward_windows": 0,
        "research_grade": "insufficient_data",
        "research_approved": False,
    }

    if X is None or y is None or len(X) < 3 or len(X) != len(y):
        return report

    sample_count = len(X)
    report["class_balance"] = round(float(np.mean(y)), 4)

    split = int(sample_count * train_test_split)
    split = max(1, min(sample_count - 1, split))
    X_tr, X_te = X[:split], X[split:]
    y_tr, y_te = y[:split], y[split:]
    report["holdout_samples"] = int(len(X_te))
    report["holdout_baseline_accuracy"] = round(_baseline_accuracy(y_te), 4)

    if len(X_te) > 0:
        holdout_model = model_factory()
        holdout_model.fit(X_tr, y_tr)
        holdout_pred = holdout_model.predict(X_te)
        report["holdout_accuracy"] = round(_accuracy(y_te, holdout_pred), 4)

    wf_train = max(min_walk_forward_train, max(40, int(sample_count * 0.4)))
    wf_window = max(10, walk_forward_window)
    wf_step = max(5, walk_forward_step)

    wf_pred_chunks = []
    wf_true_chunks = []
    windows = 0
    for train_end in range(wf_train, sample_count - 1, wf_step):
        test_end = min(sample_count, train_end + wf_window)
        if test_end <= train_end:
            continue
        model = model_factory()
        model.fit(X[:train_end], y[:train_end])
        wf_pred_chunks.append(model.predict(X[train_end:test_end]))
        wf_true_chunks.append(y[train_end:test_end])
        windows += 1

    if wf_pred_chunks:
        y_pred = np.concatenate(wf_pred_chunks)
        y_true = np.concatenate(wf_true_chunks)
        wf_acc = _accuracy(y_true, y_pred)
        wf_baseline = _baseline_accuracy(y_true)
        report["walk_forward_accuracy"] = round(wf_acc, 4)
        report["walk_forward_baseline_accuracy"] = round(wf_baseline, 4)
        report["walk_forward_edge"] = round(wf_acc - wf_baseline, 4)
        report["walk_forward_samples"] = int(len(y_true))
        report["walk_forward_windows"] = int(windows)

    if report["walk_forward_samples"] >= 60:
        if report["walk_forward_accuracy"] >= 0.55 and report["walk_forward_edge"] >= 0.01:
            report["research_grade"] = "institutional"
            report["research_approved"] = True
        elif report["walk_forward_accuracy"] >= 0.52:
            report["research_grade"] = "provisional"
        else:
            report["research_grade"] = "rejected"
    elif report["holdout_accuracy"] >= 0.52:
        report["research_grade"] = "provisional"

    return report
