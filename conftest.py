"""
conftest.py — Root pytest configuration.
Adds the project root to sys.path so all modules are importable.
Place this file in the root of your project (same level as bot.py).
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
