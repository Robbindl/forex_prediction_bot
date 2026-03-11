"""
clean_models.py — One-shot cleanup of incompatible .pkl model files.

Run ONCE from the bot folder:
    python clean_models.py

What it does:
  • Tries to load every .pkl in trained_models/ and ml_models/
  • Deletes any that fail with a sklearn/numpy version mismatch error
  • Leaves healthy models untouched
  • Prints a summary

After running, trigger retraining:
    python bot.py train --balance 50
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from logger import logger

VERSION_ERRORS = (
    '__pyx_unpickle',
    'BitGenerator',
    "Can't get attribute",
    'sklearn',
    'numpy',
    'is not a known',
    'module',
)

def is_version_mismatch(err: Exception) -> bool:
    s = str(err)
    return any(x in s for x in VERSION_ERRORS)

def clean():
    dirs = [Path('trained_models'), Path('ml_models')]
    deleted, kept, healthy = [], [], []

    for d in dirs:
        if not d.exists():
            continue
        for pkl in sorted(d.glob('*.pkl')):
            try:
                import cloudpickle
                with open(pkl, 'rb') as f:
                    cloudpickle.load(f)
                healthy.append(pkl.name)
            except Exception as e:
                if is_version_mismatch(e):
                    try:
                        pkl.unlink()
                        deleted.append(pkl.name)
                    except Exception as de:
                        logger.warning(f"Could not delete {pkl.name}: {de}")
                        kept.append(pkl.name)
                else:
                    kept.append(f"{pkl.name} ({str(e)[:60]})")

    print(f"\n{'='*55}")
    print(f"  MODEL CLEANUP COMPLETE")
    print(f"{'='*55}")
    print(f"  ✅ Healthy (kept):      {len(healthy)}")
    print(f"  🗑️  Incompatible (deleted): {len(deleted)}")
    print(f"  ⚠️  Other errors (kept): {len(kept)}")

    if deleted:
        print(f"\n  Deleted:")
        for name in deleted[:20]:
            print(f"    • {name}")
        if len(deleted) > 20:
            print(f"    ... and {len(deleted)-20} more")

    if kept:
        print(f"\n  Kept with errors:")
        for name in kept[:5]:
            print(f"    • {name}")

    print(f"\n{'='*55}")
    if deleted:
        print(f"  Now run:  python bot.py train --balance 50")
        print(f"  Or wait for midnight auto-training.")
    else:
        print(f"  No incompatible models found — nothing to clean.")
    print(f"{'='*55}\n")

if __name__ == '__main__':
    clean()
