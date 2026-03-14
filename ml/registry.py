"""ml/registry.py — Model registry: load, save, version, auto-train trigger. Replaces model_registry.py."""
from __future__ import annotations
import json, threading, time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from utils.logger import get_logger
from config.config import MODEL_DIR, MODEL_MAX_AGE_HOURS

logger = get_logger()


class ModelRegistry:
    """Tracks all trained models, their age, and triggers retraining."""

    _MANIFEST = MODEL_DIR / "registry.json"

    def __init__(self):
        self._lock     = threading.RLock()
        self._models:  Dict[str, Any]  = {}
        self._manifest: Dict[str, Dict] = {}
        self._load_manifest()

    def _load_manifest(self) -> None:
        if self._MANIFEST.exists():
            try:
                self._manifest = json.loads(self._MANIFEST.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"[Registry] Manifest load: {e}")

    def _save_manifest(self) -> None:
        try:
            self._MANIFEST.write_text(
                json.dumps(self._manifest, indent=2, default=str), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"[Registry] Manifest save: {e}")

    def register(self, name: str, model: Any, metadata: Optional[Dict] = None) -> None:
        with self._lock:
            self._models[name] = model
            self._manifest[name] = {
                "trained_at": datetime.utcnow().isoformat(),
                "metadata":   metadata or {},
            }
            self._save_manifest()
        logger.info(f"[Registry] Registered model: {name}")

    def get(self, name: str) -> Optional[Any]:
        with self._lock:
            return self._models.get(name)

    def is_stale(self, name: str) -> bool:
        info = self._manifest.get(name, {})
        trained = info.get("trained_at")
        if not trained:
            return True
        try:
            age = datetime.utcnow() - datetime.fromisoformat(trained)
            return age > timedelta(hours=MODEL_MAX_AGE_HOURS)
        except Exception:
            return True

    def load_all(self) -> None:
        """Load all .pkl / .joblib model files from MODEL_DIR."""
        try:
            import joblib
        except ImportError:
            logger.warning("[Registry] joblib not available — skipping model load")
            return
        for path in MODEL_DIR.glob("*.joblib"):
            try:
                name  = path.stem
                model = joblib.load(path)
                with self._lock:
                    self._models[name] = model
                logger.info(f"[Registry] Loaded {name} from {path.name}")
            except Exception as e:
                logger.warning(f"[Registry] Failed to load {path.name}: {e}")

    def save(self, name: str, model: Any) -> None:
        try:
            import joblib
            path = MODEL_DIR / f"{name}.joblib"
            joblib.dump(model, path)
            self.register(name, model)
            logger.info(f"[Registry] Saved {name} → {path}")
        except Exception as e:
            logger.error(f"[Registry] Save failed {name}: {e}")

    def list_models(self) -> Dict[str, Dict]:
        with self._lock:
            return {
                name: {
                    **info,
                    "loaded":  name in self._models,
                    "stale":   self.is_stale(name),
                }
                for name, info in self._manifest.items()
            }