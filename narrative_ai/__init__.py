"""
narrative_ai/__init__.py — Market Narrative AI Engine.
Provides velocity-based narrative detection via TopicClusterEngine.
Feeds from existing sentiment_analyzer, reddit_watcher, twitter_whale_watcher.
"""
from narrative_ai.topic_cluster_engine import TopicClusterEngine

_engine = TopicClusterEngine()

def get_narrative_scores() -> dict:
    return _engine.get_narrative_scores()

def get_dominant_narrative():
    return _engine.get_dominant_narrative()

def ingest(text: str, source: str = "unknown"):
    return _engine.ingest(text, source=source)