from __future__ import annotations

import random
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from models.trade_models import (
    BotPersonality, HumanExplanations,
    MemorableMoments, Trade, TradingDiary,
)
from services.db_pool import get_db
from utils.logger import logger


# ══════════════════════════════════════════════════════════════════════════════
# PersonalityDatabase — all DB reads and writes for the personality system
# ══════════════════════════════════════════════════════════════════════════════


def _trim_broker_quality(meta: Dict[str, Any]) -> Dict[str, Any]:
    raw = meta.get("broker_quality")
    if not isinstance(raw, dict) or not raw:
        return {}
    return {
        "score": round(float(raw.get("score", 0.0) or 0.0), 4),
        "primary_provider": str(raw.get("primary_provider") or ""),
        "comparison_provider": str(raw.get("comparison_provider") or ""),
        "quote_agreement_state": str(raw.get("quote_agreement_state") or ""),
        "spread_regime": str(raw.get("spread_regime") or ""),
        "quote_quality_state": str(raw.get("quote_quality_state") or ""),
        "market_state": str(raw.get("market_state") or ""),
        "market_state_transition": str(raw.get("market_state_transition") or ""),
        "market_transition_risk": round(float(raw.get("market_transition_risk", 0.0) or 0.0), 4),
        "fallback_active": bool(raw.get("fallback_active")),
    }


def _trim_market_microstructure(meta: Dict[str, Any]) -> Dict[str, Any]:
    raw = meta.get("market_microstructure")
    if not isinstance(raw, dict) or not raw:
        return {}
    return {
        "score": round(float(raw.get("score", 0.0) or 0.0), 4),
        "tick_imbalance": round(float(raw.get("tick_imbalance", 0.0) or 0.0), 4),
        "book_imbalance": round(float(raw.get("book_imbalance", 0.0) or 0.0), 4),
        "velocity_bps": round(float(raw.get("velocity_bps", 0.0) or 0.0), 4),
        "spread_bps": round(float(raw.get("spread_bps", 0.0) or 0.0), 4),
        "spread_stress": round(float(raw.get("spread_stress", 0.0) or 0.0), 4),
        "stop_hunt_risk": round(float(raw.get("stop_hunt_risk", 0.0) or 0.0), 4),
        "exhaustion_risk": round(float(raw.get("exhaustion_risk", 0.0) or 0.0), 4),
        "depth_available": bool(raw.get("depth_available")),
        "synthetic_depth_available": bool(raw.get("synthetic_depth_available")),
        "microstructure_source": str(raw.get("microstructure_source") or ""),
    }


def _trim_cross_asset_context(meta: Dict[str, Any]) -> Dict[str, Any]:
    raw = meta.get("cross_asset_context")
    if not isinstance(raw, dict) or not raw:
        return {}
    return {
        "score": round(float(raw.get("score", 0.0) or 0.0), 4),
        "confidence": round(float(raw.get("confidence", 0.0) or 0.0), 4),
        "state": str(raw.get("state") or ""),
        "supportive_direction": str(raw.get("supportive_direction") or ""),
        "dominant_peer": str(raw.get("dominant_peer") or ""),
        "dominant_relation": str(raw.get("dominant_relation") or ""),
    }

class PersonalityDatabase:
    """Thread-safe wrapper around the personality DB tables.
    Uses per-call sessions (context manager pattern) instead of a permanent
    session — prevents 'transaction already in progress' errors under
    concurrent writes and matches the pattern used by DatabaseService.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._ensure_personality_exists()

    def _get_session(self):
        """Return a fresh shared-service session context."""
        return get_db().get_session()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _ensure_personality_exists(self) -> None:
        """Create Robbie's default personality row if none exists."""
        try:
            with self._get_session() as session:
                p = session.query(BotPersonality).first()
                if not p:
                    p = BotPersonality(
                        bot_name             = "Robbie",
                        base_confidence      = 0.7,
                        cautiousness         = 0.5,
                        optimism             = 0.6,
                        talkativeness        = 0.7,
                        current_mood         = "neutral",
                        mood_emoji           = "😐",
                        consecutive_wins     = 0,
                        consecutive_losses   = 0,
                        total_trades_remembered = 0,
                        last_10_wins         = 0,
                        last_10_pnl          = 0,
                    )
                    session.add(p)
        except Exception as e:
            logger.warning(f"[Personality] Could not ensure personality row: {e}")

    # ── Trade recording ───────────────────────────────────────────────────────

    def record_trade(self, trade_data: Dict) -> Optional[int]:
        """
        Record a closed trade in the diary.
        Called by core/engine.py on_trade_closed → personality.record_trade().
        """
        with self._lock:
            try:
                meta = trade_data.get("metadata", trade_data.get("trade_metadata", {})) or {}
                if not isinstance(meta, dict):
                    meta = {}
                regime = str(meta.get("regime", "unknown")).lower()
                rsi = float(meta.get("rsi", 0) or 0)
                setup_type = _classify_setup(regime, rsi, trade_data.get("exit_reason", ""))
                review = meta.get("post_trade_review")
                if not isinstance(review, dict) or not review:
                    try:
                        from services.post_trade_review_service import get_service as get_post_trade_review_service

                        review = get_post_trade_review_service().build_review(
                            {
                                **trade_data,
                                "metadata": meta,
                            }
                        )
                        if isinstance(review, dict) and review:
                            meta["post_trade_review"] = review
                    except Exception:
                        review = {}

                execution_feedback = meta.get("execution_feedback")
                if not isinstance(execution_feedback, dict):
                    execution_feedback = {}
                setup_memory = meta.get("setup_memory")
                if not isinstance(setup_memory, dict):
                    setup_memory = {}
                broker_quality = _trim_broker_quality(meta)
                market_microstructure = _trim_market_microstructure(meta)
                cross_asset_context = _trim_cross_asset_context(meta)
                entry_diagnostics = review.get("entry_diagnostics") if isinstance(review, dict) else {}
                if not isinstance(entry_diagnostics, dict):
                    entry_diagnostics = {}

                with self._get_session() as session:
                    entry = TradingDiary(
                        asset         = trade_data.get("asset", ""),
                        trade_id      = trade_data.get("trade_id"),
                        setup_type    = setup_type,
                        pnl           = float(trade_data.get("pnl", 0)),
                        exit_reason   = trade_data.get("exit_reason", ""),
                        entry_price   = float(trade_data.get("entry_price", 0) or 0),
                        exit_price    = float(trade_data.get("exit_price",  0) or 0),
                        confidence    = float(trade_data.get("confidence",  0.5) or 0.5),
                        rsi_at_entry  = float(rsi) if rsi else None,
                        market_regime = regime,
                        notes         = {
                            "strategy_id": trade_data.get("strategy_id", ""),
                            "category":    trade_data.get("category", ""),
                            "direction":   trade_data.get("direction", trade_data.get("signal", "")),
                            "post_trade_review": review,
                            "execution_feedback": execution_feedback,
                            "setup_memory": {
                                "memory_score": meta.get("memory_score", setup_memory.get("memory_score")),
                                "memory_edge": meta.get("memory_edge", setup_memory.get("memory_edge")),
                                "sample_count": meta.get("memory_sample_count", setup_memory.get("sample_count")),
                            },
                            "broker_quality": broker_quality,
                            "market_microstructure": market_microstructure,
                            "cross_asset_context": cross_asset_context,
                            "entry_diagnostics": entry_diagnostics,
                        },
                    )
                    session.add(entry)
                    session.flush()
                    entry_id = entry.id

                self._update_mood_from_trade(trade_data)
                self._check_memorable(trade_data, setup_type)

                return entry_id

            except Exception as e:
                logger.error(f"[Personality] record_trade failed: {e}")
                return None

    def _update_mood_from_trade(self, trade_data: Dict) -> None:
        """Shift Robbie's mood based on the closed trade."""
        try:
            with self._get_session() as session:
                p = session.query(BotPersonality).first()
                if not p:
                    return
                pnl = float(trade_data.get("pnl", 0))
                win = pnl > 0

                if win:
                    p.consecutive_wins += 1
                    p.consecutive_losses = 0
                else:
                    p.consecutive_losses += 1
                    p.consecutive_wins = 0

                p.total_trades_remembered += 1

                recent = (
                    session.query(TradingDiary)
                    .order_by(TradingDiary.created_at.desc())
                    .limit(10).all()
                )
                if recent:
                    p.last_10_wins = sum(1 for t in recent if t.pnl and t.pnl > 0)
                    p.last_10_pnl = sum(float(t.pnl) for t in recent if t.pnl)

                p.current_mood, p.mood_emoji = _calculate_mood(p)
        except Exception as e:
            logger.debug(f"[Personality] mood update failed: {e}")

    def _check_memorable(self, trade_data: Dict, setup_type: str) -> None:
        """Save a MemorableMoment if this trade crosses a significance threshold."""
        try:
            pnl = float(trade_data.get("pnl", 0))
            asset = trade_data.get("asset", "")
            is_win = pnl > 0

            title = None
            if pnl > 50:
                title = f"🏆 Big win on {asset} — +${pnl:.0f}"
            elif pnl < -30:
                title = f"💀 Took a hit on {asset} — -${abs(pnl):.0f}"
            elif trade_data.get("confidence", 0) >= 0.90:
                title = f"🎯 Max-confidence {asset} {'win' if is_win else 'loss'}"

            if not title:
                return

            with self._get_session() as session:
                moment = MemorableMoments(
                    moment_date  = datetime.now(),
                    title        = title,
                    description  = trade_data.get("exit_reason", ""),
                    asset        = asset,
                    pnl          = pnl,
                    is_win       = is_win,
                    is_memorable = True,
                    tags         = {
                        "setup":      setup_type,
                        "strategy":   trade_data.get("strategy_id", ""),
                        "confidence": trade_data.get("confidence", 0),
                        "regime":     (trade_data.get("metadata") or {}).get("regime", ""),
                    },
                )
                session.add(moment)
        except Exception as e:
            logger.debug(f"[Personality] memorable check failed: {e}")

    # ── Historical context ────────────────────────────────────────────────────

    def find_similar_setups(self, asset: str, setup_type: str,
                             days_back: int = 30) -> List[Dict]:
        """Return past diary entries for the same asset+setup."""
        try:
            cutoff  = datetime.now() - timedelta(days=days_back)
            with self._get_session() as session:
                rows = (
                    session.query(TradingDiary)
                    .filter(
                        TradingDiary.asset      == asset,
                        TradingDiary.setup_type == setup_type,
                        TradingDiary.created_at >= cutoff,
                    )
                    .order_by(TradingDiary.created_at.desc())
                    .limit(10).all()
                )
            return [
                {
                    "date":       r.created_at.strftime("%Y-%m-%d %H:%M"),
                    "pnl":        float(r.pnl) if r.pnl else 0.0,
                    "was_win":    bool(r.pnl and r.pnl > 0),
                    "confidence": float(r.confidence) if r.confidence else 0.0,
                    "exit_reason":r.exit_reason or "",
                }
                for r in rows
            ]
        except Exception as e:
            logger.debug(f"[Personality] find_similar_setups failed: {e}")
            return []

    def get_historical_context(self, asset: str, setup_type: str) -> Optional[str]:
        """Return a one-line human-readable historical context string."""
        similar = self.find_similar_setups(asset, setup_type)
        if not similar:
            return None
        wins     = sum(1 for s in similar if s["was_win"])
        total    = len(similar)
        win_rate = wins / total

        if win_rate > 0.70:
            return f"This setup has worked well for {asset} lately ({wins}/{total} wins) 📈"
        if win_rate > 0.50:
            return f"Mixed history on {asset} with this setup — {wins}/{total} wins 🤷"
        losses = [s for s in similar if not s["was_win"]]
        if losses:
            worst = min(losses, key=lambda x: x["pnl"])
            day   = worst["date"].split(" ")[0]
            return f"Careful — last time this setup hit on {day} we lost ${abs(worst['pnl']):.0f} on {asset} ⚠️"
        return None

    def get_asset_memory(self, asset: str, days_back: int = 14) -> Dict:
        """Return Robbie's recent memory for a specific asset."""
        try:
            cutoff = datetime.now() - timedelta(days=days_back)
            with self._get_session() as session:
                rows = (
                    session.query(TradingDiary)
                    .filter(TradingDiary.asset == asset, TradingDiary.created_at >= cutoff)
                    .order_by(TradingDiary.created_at.desc())
                    .limit(20).all()
                )
            if not rows:
                return {"has_memory": False}

            wins      = sum(1 for r in rows if r.pnl and r.pnl > 0)
            total_pnl = sum(float(r.pnl) for r in rows if r.pnl)
            avg_conf  = sum(float(r.confidence) for r in rows if r.confidence) / len(rows)
            regimes   = [r.market_regime for r in rows if r.market_regime]
            setups    = [r.setup_type for r in rows if r.setup_type]

            return {
                "has_memory":    True,
                "total_trades":  len(rows),
                "wins":          wins,
                "losses":        len(rows) - wins,
                "win_rate":      round(wins / len(rows) * 100, 1),
                "total_pnl":     round(total_pnl, 2),
                "avg_confidence":round(avg_conf * 100, 1),
                "last_seen":     rows[0].created_at.strftime("%Y-%m-%d"),
                "last_pnl":      float(rows[0].pnl) if rows[0].pnl else 0,
                "common_regime": max(set(regimes), key=regimes.count) if regimes else "unknown",
                "common_setup":  max(set(setups),  key=setups.count)  if setups  else "unknown",
            }
        except Exception as e:
            logger.debug(f"[Personality] get_asset_memory failed: {e}")
            return {"has_memory": False}

    # ── Personality report ────────────────────────────────────────────────────

    def get_personality_report(self) -> Dict:
        """Full personality snapshot for /mood, /diary, and dashboard."""
        try:
            with self._get_session() as session:
                p = session.query(BotPersonality).first()

                last_week = datetime.now() - timedelta(days=7)
                weekly_total = session.query(TradingDiary).filter(
                    TradingDiary.created_at >= last_week
                ).count()
                weekly_wins = session.query(TradingDiary).filter(
                    TradingDiary.created_at >= last_week,
                    TradingDiary.pnl > 0,
                ).count()

                # Fallback: if TradingDiary is empty, read directly from trades table.
                # Diary entries only exist when record_trade() was called — trades that
                # closed before personality service started (gap-fill, offline SL/TP)
                # never get diary entries but ARE in the trades table.
                if weekly_total == 0:
                    try:
                        weekly_total = session.query(Trade).filter(
                            Trade.exit_time >= last_week,
                            Trade.exit_time.isnot(None),
                        ).count()
                        weekly_wins = session.query(Trade).filter(
                            Trade.exit_time >= last_week,
                            Trade.exit_time.isnot(None),
                            Trade.pnl > 0,
                        ).count()
                    except Exception:
                        pass

                moments = (
                    session.query(MemorableMoments)
                    .order_by(MemorableMoments.moment_date.desc())
                    .limit(5).all()
                )

                return {
                    "name":         p.bot_name if p else "Robbie",
                    "current_mood": p.current_mood if p else "neutral",
                    "mood_emoji":   p.mood_emoji   if p else "😐",
                    "traits": {
                        "base_confidence": float(p.base_confidence) if p else 0.7,
                        "cautiousness":    float(p.cautiousness)    if p else 0.5,
                        "optimism":        float(p.optimism)        if p else 0.6,
                        "talkativeness":   float(p.talkativeness)   if p else 0.7,
                    },
                    "stats": {
                        "total_trades_remembered": p.total_trades_remembered if p else 0,
                        "consecutive_wins":        p.consecutive_wins         if p else 0,
                        "consecutive_losses":      p.consecutive_losses       if p else 0,
                        "last_10_wins":            p.last_10_wins             if p else 0,
                        "last_10_pnl":   float(p.last_10_pnl) if p and p.last_10_pnl else 0.0,
                        "weekly_trades":  weekly_total,
                        "weekly_win_rate":(weekly_wins / weekly_total * 100) if weekly_total else 0,
                    },
                    "memorable_moments": [
                        {
                            "title":  m.title,
                            "date":   m.moment_date.strftime("%Y-%m-%d"),
                            "asset":  m.asset,
                            "pnl":    float(m.pnl) if m.pnl else 0.0,
                            "is_win": m.is_win,
                        }
                        for m in moments
                    ],
                }
        except Exception as e:
            logger.error(f"[Personality] get_personality_report failed: {e}")
            return _default_report()

    # ── Explanation storage ───────────────────────────────────────────────────

    def save_explanation(self, data: Dict) -> Optional[int]:
        try:
            text = data.get("text", "")
            with self._get_session() as session:
                row = HumanExplanations(
                    asset            = data.get("asset", ""),
                    explanation_text = text[:4000],
                    direction        = data.get("direction", ""),
                    confidence       = float(data.get("confidence", 0)),
                    rsi_value        = data.get("rsi"),
                    volume_value     = data.get("volume"),
                    news_count       = int(data.get("news_count", 0)),
                    sentiment_score  = float(data.get("sentiment", 0)),
                    sent_to_telegram = bool(data.get("sent_to_telegram", False)),
                    telegram_chat_id = data.get("chat_id"),
                )
                session.add(row)
                session.flush()
                return row.id
        except Exception as e:
            logger.debug(f"[Personality] save_explanation failed: {e}")
            return None

    def close(self) -> None:
        pass  # no permanent session to close anymore


# ══════════════════════════════════════════════════════════════════════════════
# RobbieExplainer — generates all of Robbie's spoken text
# ══════════════════════════════════════════════════════════════════════════════

class RobbieExplainer:
    """
    Generates plain-English signal explanations and answers free-form
    questions about any asset. All responses are in Robbie's voice.

    Usage:
        explainer = RobbieExplainer()
        text = explainer.explain_signal(asset, df, signal_dict)
        text = explainer.answer(asset, question, signal_dict)
    """

    def __init__(self):
        self.db = PersonalityDatabase()

    # ── Signal explanation ─────────────────────────────────────────────────────

    def explain_signal(
        self,
        asset:    str,
        df:       Optional[pd.DataFrame],
        signal:   Dict,
        news:     Optional[List[Dict]] = None,
        sentiment:Optional[Dict]       = None,
        chat_id:  Optional[str]        = None,
        analysis: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Generate Robbie's full signal explanation.
        Called by /why command and the dashboard overlay.
        """
        signal = signal or {}
        report    = self.db.get_personality_report()
        mood      = report["current_mood"]
        if analysis and self._current_signal(signal, analysis) is None:
            memory = self.db.get_asset_memory(asset)
            return _render_market_state_response(
                asset,
                analysis,
                df,
                mood,
                memory,
                topic="why",
            )

        direction = signal.get("direction", signal.get("signal", "HOLD"))
        confidence= float(signal.get("confidence", 0.5)) * 100
        entry     = float(signal.get("entry_price", 0))
        sl        = float(signal.get("stop_loss", 0))
        tp        = float(signal.get("take_profit", 0))
        meta      = signal.get("metadata", {}) or {}
        regime    = meta.get("regime", "unknown")
        sess      = meta.get("session", "")
        rr        = signal.get("risk_reward", signal.get("rr_ratio", 0))

        parts = _explanation_headline(asset, direction, confidence, mood, report["name"])

        # ── Technical reasons from indicators ────────────────────────────────
        if df is not None and not df.empty:
            reasons = _technical_reasons(df, signal)
            if reasons:
                parts.append("\n*Why I think that:*")
                for r in reasons[:4]:
                    parts.append(f"  • {r}")

        # ── Historical context from Robbie's memory ──────────────────────────
        setup_type = _classify_setup(regime, _last_rsi(df), signal.get("exit_reason", ""))
        history    = self.db.get_historical_context(asset, setup_type)
        if history:
            parts.append(f"\n📝 *Robbie's diary:* {history}")

        # ── Asset memory summary ─────────────────────────────────────────────
        memory = self.db.get_asset_memory(asset)
        if memory.get("has_memory"):
            parts.append(
                f"\n🧠 *My history on {asset}:*\n"
                f"  {memory['total_trades']} trades | "
                f"{memory['win_rate']}% win rate | "
                f"${memory['total_pnl']:+.2f} P&L"
            )

        # ── Trade levels ─────────────────────────────────────────────────────
        parts.extend(_explanation_trade_levels(entry, sl, tp, rr))

        # ── Context ──────────────────────────────────────────────────────────
        context_line = _explanation_context_line(regime, sess)
        if context_line:
            parts.append(context_line)

        # ── News ─────────────────────────────────────────────────────────────
        parts.extend(_explanation_news_lines(news))

        # ── Sign-off ─────────────────────────────────────────────────────────
        parts.append("\n" + _signoff(mood))

        text = "\n".join(parts)

        # Save to DB for analytics
        try:
            self.db.save_explanation({
                "asset":          asset,
                "text":           text,
                "direction":      direction,
                "confidence":     confidence / 100,
                "rsi":            _last_rsi(df),
                "news_count":     len(news) if news else 0,
                "sentiment":      sentiment.get("score", 0) if sentiment else 0,
                "sent_to_telegram": chat_id is not None,
                "chat_id":        chat_id,
            })
        except Exception:
            pass

        return text

    # ── /ask — free-form question answering ───────────────────────────────────

    def answer(
        self,
        asset:    str,
        question: str,
        signal:   Optional[Dict] = None,
        df:       Optional[pd.DataFrame] = None,
        analysis: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Answer a free-form question about an asset in Robbie's voice.
        Called by the /ask Telegram command.
        """
        report  = self.db.get_personality_report()
        mood    = report["current_mood"]
        memory  = self.db.get_asset_memory(asset)
        q_lower = question.lower()
        live_signal = self._current_signal(signal, analysis)

        # ── Route to the right answer type ───────────────────────────────────

        if any(w in q_lower for w in ("buy", "sell", "should i", "enter", "trade")):
            if analysis and live_signal is None:
                return self._answer_analysis_state(asset, mood, memory, analysis, df=df, topic="trade")
            return self._answer_trade_question(asset, question, live_signal, df, mood, memory)

        if any(w in q_lower for w in ("confidence", "confident", "conviction", "sure")):
            if analysis and live_signal is None:
                return self._answer_analysis_state(asset, mood, memory, analysis, df=df, topic="confidence")
            return self._answer_confidence_question(asset, live_signal, mood, memory)

        if any(w in q_lower for w in ("remember", "history", "last time", "before", "previously", "diary")):
            return self._answer_memory_question(asset, mood, memory)

        if any(w in q_lower for w in ("feel", "mood", "nervous", "worried")):
            return self._answer_mood_question(asset, mood, report)

        if any(w in q_lower for w in ("why", "reason", "explain", "because")):
            if live_signal and df is not None:
                return self.explain_signal(asset, df, live_signal, analysis=analysis)
            if analysis:
                return self._answer_analysis_state(asset, mood, memory, analysis, df=df, topic="why")
            return self._answer_why_no_signal(asset, mood)

        if any(w in q_lower for w in ("risk", "stop", "loss", "safe")):
            if analysis and live_signal is None:
                return self._answer_analysis_state(asset, mood, memory, analysis, df=df, topic="risk")
            return self._answer_risk_question(asset, live_signal, mood, memory)

        if any(w in q_lower for w in ("news", "sentiment", "social", "twitter")):
            if analysis and live_signal is None:
                return self._answer_analysis_state(asset, mood, memory, analysis, df=df, topic="sentiment")
            return self._answer_sentiment_question(asset, live_signal, mood)

        # Default — general take
        if analysis and live_signal is None:
            return self._answer_analysis_state(asset, mood, memory, analysis, df=df, topic="general")
        return self._answer_general(asset, question, live_signal, mood, memory)

    @staticmethod
    def _current_signal(signal: Optional[Dict], analysis: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        candidate: Any = signal
        if (not isinstance(candidate, dict) or not candidate) and isinstance(analysis, dict):
            maybe_signal = analysis.get("signal")
            if isinstance(maybe_signal, dict):
                candidate = maybe_signal
        if not isinstance(candidate, dict) or not candidate:
            return None
        if str(candidate.get("direction", "HOLD") or "HOLD").upper() == "HOLD":
            return None
        if candidate.get("alive") is False:
            return None
        return candidate

    def describe_market_state(
        self,
        asset: str,
        analysis: Dict[str, Any],
        *,
        df: Optional[pd.DataFrame] = None,
        topic: str = "general",
    ) -> str:
        report = self.db.get_personality_report()
        memory = self.db.get_asset_memory(asset)
        return _render_market_state_response(
            asset,
            analysis,
            df,
            report.get("current_mood", "neutral"),
            memory,
            topic=topic,
        )

    def _answer_analysis_state(
        self,
        asset: str,
        mood: str,
        memory: Dict[str, Any],
        analysis: Dict[str, Any],
        *,
        df: Optional[pd.DataFrame] = None,
        topic: str = "general",
    ) -> str:
        return _render_market_state_response(asset, analysis, df, mood, memory, topic=topic)

    def _answer_trade_question(self, asset, question, signal, df, mood, memory) -> str:
        if not signal or signal.get("direction", "HOLD") == "HOLD":
            no_sig = random.choice([
                f"Honestly? I'm not seeing a clean entry on {asset} right now. 🤷",
                f"The decision engine isn't giving me a clear signal on {asset} at the moment.",
                f"Nothing's jumping out at me on {asset}. I'd wait for a better setup.",
            ])
            if memory.get("has_memory"):
                no_sig += f"\n\nFor context — I've traded {asset} {memory['total_trades']} times recently with {memory['win_rate']}% wins."
            return no_sig

        direction = signal.get("direction", "BUY")
        conf      = float(signal.get("confidence", 0.5)) * 100
        entry     = float(signal.get("entry_price", 0))
        sl        = float(signal.get("stop_loss", 0))
        tp        = float(signal.get("take_profit", 0))
        rr        = float(signal.get("risk_reward", signal.get("rr_ratio", 0)))

        # Mood-influenced directness
        if mood in ("euphoric", "confident", "on_fire"):
            opener = random.choice([
                f"Yeah, I like this one! {_conf_emoji(conf)}",
                f"This is looking solid to me.",
                f"Strong signal here — I'd take it.",
            ])
        elif mood in ("cautious", "shaken"):
            opener = random.choice([
                f"I see the setup, but I'm being careful right now.",
                f"There's a signal, though I'm not super aggressive this week.",
                f"It looks okay — just keeping my position sizes conservative.",
            ])
        else:
            opener = random.choice([
                f"There's a {direction} setup on {asset}.",
                f"I'm seeing a {direction} signal here.",
                f"The decision engine flagged {asset} {direction.lower()}.",
            ])

        lines = [opener, ""]
        lines.append(f"*{direction} {asset}* at `{entry:.5f}`")
        if sl:  lines.append(f"  Stop: `{sl:.5f}`")
        if tp:  lines.append(f"  Target: `{tp:.5f}`")
        if rr:  lines.append(f"  R:R: {rr:.1f}:1")
        lines.append(f"  Confidence: {conf:.0f}%")

        # Add memory context
        if memory.get("has_memory") and memory["total_trades"] >= 3:
            lines.append(
                f"\n📝 I've traded {asset} {memory['total_trades']}x recently — "
                f"{memory['win_rate']}% win rate, ${memory['total_pnl']:+.2f} total."
            )

        if df is not None and not df.empty:
            reasons = _technical_reasons(df, signal)
            if reasons:
                lines.append("\n*Why:*")
                for r in reasons[:3]:
                    lines.append(f"  • {r}")

        return "\n".join(lines)

    def _answer_memory_question(self, asset, mood, memory) -> str:
        if not memory.get("has_memory"):
            return (
                f"Hmm, I don't have any recent trades on {asset} in my diary. "
                f"Either I haven't traded it lately or the DB is fresh. 🤔"
            )
        opener = random.choice([
            f"Let me check my diary on {asset}... 📔",
            f"Yeah, I remember {asset}.",
            f"I've got some history on {asset}.",
        ])
        lines = [opener, ""]
        lines.append(f"*Last {memory['total_trades']} trades on {asset}:*")
        lines.append(f"  Wins/Losses: {memory['wins']}/{memory['losses']}")
        lines.append(f"  Win rate: {memory['win_rate']}%")
        lines.append(f"  Total P&L: ${memory['total_pnl']:+.2f}")
        lines.append(f"  Avg confidence: {memory['avg_confidence']}%")
        lines.append(f"  Last traded: {memory['last_seen']}")
        lines.append(f"  Last P&L: ${memory['last_pnl']:+.2f}")
        if memory.get("common_regime") and memory["common_regime"] != "unknown":
            lines.append(f"  Common regime: {memory['common_regime'].replace('_', ' ')}")
        return "\n".join(lines)

    def _answer_mood_question(self, asset, mood, report) -> str:
        stats   = report["stats"]
        emoji   = report["mood_emoji"]
        lines   = [f"Right now I'm feeling *{mood.upper()}* {emoji}\n"]
        if stats["consecutive_wins"] > 1:
            lines.append(f"🔥 On a {stats['consecutive_wins']}-trade winning streak!")
        elif stats["consecutive_losses"] > 1:
            lines.append(f"😰 Coming off {stats['consecutive_losses']} losses in a row — being careful.")
        lines.append(f"\nThis week: {stats['weekly_win_rate']:.0f}% win rate")
        lines.append(f"Last 10: {stats['last_10_wins']}/10 wins | ${stats['last_10_pnl']:+.2f} P&L")

        # Mood-specific comment on the asset
        if mood in ("euphoric", "confident", "on_fire"):
            lines.append(f"\nOn {asset} — I'm in a good headspace, willing to be aggressive if the signal is right.")
        elif mood in ("cautious", "shaken", "grumpy"):
            lines.append(f"\nOn {asset} — I'm keeping things tight right now. Only A+ setups for me.")
        else:
            lines.append(f"\nOn {asset} — feeling balanced, just going with what the data says.")
        return "\n".join(lines)

    def _answer_why_no_signal(self, asset, mood) -> str:
        return (
            f"I don't have an active signal on {asset} right now, so I can't give you a full breakdown. "
            f"Run `/signal {asset}` to check the live decision engine, then `/ask {asset} why` once a signal is active."
        )

    def _answer_risk_question(self, asset, signal, mood, memory) -> str:
        lines = []
        if signal and signal.get("stop_loss"):
            entry = float(signal.get("entry_price", 0))
            sl    = float(signal.get("stop_loss", 0))
            tp    = float(signal.get("take_profit", 0))
            rr    = float(signal.get("risk_reward", signal.get("rr_ratio", 0)))
            risk_pct = abs(entry - sl) / entry * 100 if entry else 0
            lines.append(f"*Risk profile for {asset}:*\n")
            lines.append(f"  Stop loss distance: {risk_pct:.2f}% from entry")
            if tp and entry:
                reward_pct = abs(tp - entry) / entry * 100
                lines.append(f"  Reward distance: {reward_pct:.2f}%")
            if rr:
                lines.append(f"  R:R ratio: {rr:.1f}:1")
                if rr >= 2:
                    lines.append(f"  ✅ R:R is solid — I like this risk setup")
                elif rr >= 1.5:
                    lines.append(f"  🟡 R:R is acceptable")
                else:
                    lines.append(f"  ⚠️ R:R is a bit low — I'd want at least 1.5:1")
        else:
            lines.append(f"No active signal on {asset} so I can't quote specific levels.")

        if memory.get("has_memory") and memory["total_trades"] >= 3:
            win_rate = memory["win_rate"]
            if win_rate >= 60:
                lines.append(f"\nHistorically {asset} has been good to me — {win_rate}% win rate.")
            elif win_rate <= 40:
                lines.append(f"\nFair warning — {asset} has been tough for me recently, only {win_rate}% wins.")

        if mood in ("cautious", "shaken"):
            lines.append("\nGiven how things have been going, I'd keep position sizes small.")

        return "\n".join(lines) if lines else f"No active signal on {asset} to assess risk against."

    def _answer_confidence_question(self, asset, signal, mood, memory) -> str:
        if not signal or signal.get("direction", "HOLD") == "HOLD":
            return (
                f"I don't have an active trade setup on {asset} right now, so there isn't a live confidence score to quote. "
                f"Run `/signal {asset}` first, then ask again."
            )

        direction = str(signal.get("direction", "HOLD") or "HOLD").upper()
        confidence = float(signal.get("confidence", 0.0) or 0.0) * 100
        rr = float(signal.get("risk_reward", signal.get("rr_ratio", 0.0)) or 0.0)
        meta = signal.get("metadata", {}) or {}
        governance = meta.get("governance_validation", {}) or {}
        grade = str(
            meta.get("governance_grade")
            or governance.get("grade")
            or ""
        ).strip().upper()
        exec_quality = float(meta.get("execution_quality_score", 0.0) or 0.0)

        lines = [f"Current confidence on *{asset}* is *{confidence:.0f}%* for a *{direction}* setup."]
        if confidence >= 80:
            lines.append("That is one of the stronger reads on the board right now.")
        elif confidence >= 65:
            lines.append("That is a workable read, but not the cleanest signal in the universe.")
        else:
            lines.append("That is not a strong read, so I would treat it carefully.")

        details = []
        if rr > 0:
            details.append(f"reward to risk is `{rr:.2f}:1`")
        if grade:
            details.append(f"governance came through at grade `{grade}`")
        if exec_quality > 0:
            details.append(f"recent execution quality on similar setups is `{exec_quality:.0f}/100`")
        if details:
            lines.append("Right now " + ", ".join(details) + ".")

        if memory.get("has_memory") and memory.get("total_trades", 0) >= 3:
            lines.append(
                f"I've traded {asset} {memory['total_trades']} times recently with a {memory['win_rate']}% win rate."
            )

        if mood in ("cautious", "shaken"):
            lines.append("Because recent form has been shaky, I would still keep sizing controlled even if the setup is valid.")

        return "\n".join(lines)

    def _answer_sentiment_question(self, asset, signal, mood) -> str:
        meta = (signal or {}).get("metadata", {}) or {}
        sentiment_score = meta.get("sentiment_score")
        market_intel_score = meta.get("market_intelligence_score")
        score = sentiment_score if sentiment_score is not None else market_intel_score
        sources = list(
            meta.get("sentiment_sources")
            or meta.get("market_intelligence_sources")
            or []
        )
        narrative = _narrative_label(meta.get("narrative") or meta.get("dominant_narrative"))
        whale = str(meta.get("whale_dominant") or "").upper()

        if score is None and not sources and not narrative and whale not in {"BUY", "SELL"}:
            return (
                f"I don't have a live sentiment snapshot for {asset} right now. "
                f"Run `/signal {asset}` or `/why {asset}` after the decision engine has reviewed it."
            )

        score_value = float(score or 0.0)
        lines = [f"*Sentiment read on {asset}:*"]
        lines.append(f"  {_describe_sentiment_score(score_value)}")

        if whale in {"BUY", "SELL"}:
            flow_word = "bullish" if whale == "BUY" else "bearish"
            lines.append(f"  Whale flow currently leans {flow_word}.")

        if narrative:
            lines.append(f"  The main narrative in the background is {narrative}.")

        if sources:
            lines.append(
                f"  This read is coming from {len(sources)} source{'s' if len(sources) != 1 else ''}: "
                f"{', '.join(str(item) for item in sources[:4])}."
            )

        if mood in ("cautious", "shaken") and score_value < 0.15:
            lines.append("It is not a decisive sentiment edge, so I would not lean on it too heavily by itself.")

        return "\n".join(lines)

    def _answer_general(self, asset, question, signal, mood, memory) -> str:
        lines = []
        name  = asset.replace("-USD", "").replace("/USD", "").replace("=F", "")

        # General opener
        if memory.get("has_memory"):
            lines.append(
                f"Good question about {name}. I've traded it {memory['total_trades']}x "
                f"recently with {memory['win_rate']}% wins.\n"
            )
        else:
            lines.append(f"Interesting question about {name}.\n")

        # Add signal context if available
        if signal and signal.get("direction", "HOLD") != "HOLD":
            d    = signal["direction"]
            conf = float(signal.get("confidence", 0.5)) * 100
            lines.append(f"My current read: *{d}* with a signal score of {conf:.0f}/100.")

        # Mood-flavoured closing
        if mood in ("euphoric", "on_fire"):
            lines.append(f"\nI'm feeling good about the markets today — happy to take the right setup on {name}.")
        elif mood in ("cautious", "shaken"):
            lines.append(f"\nBeing a bit selective right now — I'd want to see a really clean setup on {name} before committing.")
        else:
            lines.append(f"\nJust following the data on {name} — no personal bias either way.")

        lines.append(f"\nAnything more specific? Try `/signal {asset}` or `/why {asset}`.")
        return "\n".join(lines)

    def close(self) -> None:
        self.db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Module-level singleton — wired by core/engine.py on startup
# ══════════════════════════════════════════════════════════════════════════════

class _PersonalitySingleton:
    """Thin wrapper so core/engine.py can call personality.record_trade() safely."""

    def __init__(self):
        self._db:       Optional[PersonalityDatabase] = None
        self._lock      = threading.Lock()

    def _get_db(self) -> PersonalityDatabase:
        with self._lock:
            if self._db is None:
                self._db = PersonalityDatabase()
            return self._db

    def record_trade(self, trade_dict: Dict) -> None:
        """Called from core/engine.py on_trade_closed."""
        try:
            self._get_db().record_trade(trade_dict)
        except Exception as e:
            logger.debug(f"[Personality] record_trade skipped: {e}")

    def get_report(self) -> Dict:
        try:
            return self._get_db().get_personality_report()
        except Exception:
            return _default_report()


personality: _PersonalitySingleton = _PersonalitySingleton()


# ══════════════════════════════════════════════════════════════════════════════
# Pure helpers — no DB access
# ══════════════════════════════════════════════════════════════════════════════

def _classify_setup(regime: str, rsi: float, exit_reason: str) -> str:
    if "trending_up" in regime or "trending_down" in regime:
        return "trend_follow"
    if rsi and rsi < 35:
        return "oversold_bounce"
    if rsi and rsi > 65:
        return "overbought_fade"
    if "Take Profit" in (exit_reason or ""):
        return "breakout"
    return "pullback"


def _calculate_mood(p: BotPersonality):
    if p.consecutive_wins  >= 5:  return "euphoric", "🚀🚀🚀"
    if p.consecutive_wins  >= 3:  return "confident", "😎"
    if p.consecutive_losses >= 4: return "shaken",    "😰"
    if p.consecutive_losses >= 2: return "cautious",  "🤔"
    if p.last_10_wins       >= 8: return "on_fire",   "🔥"
    if p.last_10_pnl and float(p.last_10_pnl) > 100: return "rich",   "🤑"
    if p.last_10_pnl and float(p.last_10_pnl) < -50: return "grumpy", "😤"
    return "neutral", "😐"


def _default_report() -> Dict:
    return {
        "name": "Robbie", "current_mood": "neutral", "mood_emoji": "😐",
        "traits": {"base_confidence": 0.7, "cautiousness": 0.5, "optimism": 0.6, "talkativeness": 0.7},
        "stats": {"total_trades_remembered": 0, "consecutive_wins": 0, "consecutive_losses": 0,
                  "last_10_wins": 0, "last_10_pnl": 0.0, "weekly_trades": 0, "weekly_win_rate": 0},
        "memorable_moments": [],
    }


def _last_rsi(df) -> float:
    try:
        return float(df["rsi"].iloc[-1]) if df is not None and "rsi" in df.columns else 50.0
    except Exception:
        return 50.0


def _conf_emoji(confidence: float) -> str:
    if confidence >= 88: return "🚀🚀🚀"
    if confidence >= 78: return "🚀🚀"
    if confidence >= 68: return "🚀"
    if confidence >= 58: return "🤔"
    return "🤷"


def _confidence_line(confidence: float, mood: str) -> str:
    if mood in ("euphoric", "on_fire") and confidence > 75:
        return f"And the signal score is elevated here — {confidence:.0f}/100. 🔥"
    if mood in ("cautious", "shaken") and confidence < 75:
        return f"Signal score is {confidence:.0f}/100 — I'd keep the size small given how this week has gone."
    if confidence > 80:
        return f"This is one of the stronger reads on the board ({confidence:.0f}/100)."
    if confidence > 65:
        return f"Decent signal score here ({confidence:.0f}/100)."
    if confidence > 52:
        return f"Not a top-tier score ({confidence:.0f}/100), but worth watching."
    return f"Honestly a weak read ({confidence:.0f}/100) — market's being weird."


def _describe_sentiment_score(score: float) -> str:
    if score >= 0.35:
        return f"Sentiment is strongly bullish at {score:+.2f}."
    if score >= 0.12:
        return f"Sentiment leans bullish at {score:+.2f}."
    if score <= -0.35:
        return f"Sentiment is strongly bearish at {score:+.2f}."
    if score <= -0.12:
        return f"Sentiment leans bearish at {score:+.2f}."
    return f"Sentiment is close to neutral at {score:+.2f}."


def _narrative_label(value: Any) -> str:
    labels = {
        "AI_TOKENS": "AI-related crypto narrative",
        "HALVING_BUZZ": "halving narrative",
    }
    raw = str(value or "").strip()
    if not raw:
        return ""
    return labels.get(raw, raw.replace("_", " ").lower())


def _mood_greeting(mood: str, name: str) -> str:
    greetings = {
        "euphoric":  [f"Yo {name}! Absolutely loving the markets today 🚀", f"What a day! Let me break this down 🚀🚀"],
        "confident": [f"Hey {name}! Got something good for you 😎", f"Looking at some solid setups right now."],
        "on_fire":   [f"🔥 On fire today {name}! Here's what I'm seeing:", f"Been a great run — let's keep it going!"],
        "rich":      [f"Good vibes only {name} 🤑 — here's the read:", f"Markets have been kind. Let's talk {{}}."],
        "cautious":  [f"Hey {name}, being careful right now 🤔 —", f"Not being too aggressive this week, but here's what I see:"],
        "shaken":    [f"Rough stretch lately {name} 😰 — being selective here:", f"Taking it carefully — here's my honest read:"],
        "grumpy":    [f"Markets have been rough 😤 — let's cut to it:", f"Not in the best mood but here's what I see:"],
        "neutral":   [f"Hey {name}! 👋 Here's the breakdown:", f"Quick update for you:", f"Let me walk you through this:"],
    }
    options = greetings.get(mood, greetings["neutral"])
    return random.choice(options)


def _signoff(mood: str) -> str:
    signoffs = {
        "euphoric":  ["Let's get it! 🚀", "This is what we live for! 💰"],
        "confident": ["That's my read. 😎", "Solid setup — let's see it play out."],
        "on_fire":   ["Keep riding the wave! 🔥", "Stack those wins! 💪"],
        "rich":      ["Money moves only 🤑", "Let the profits compound! 💸"],
        "cautious":  ["Stay disciplined. 🤔", "Manage the risk first."],
        "shaken":    ["One trade at a time. 😰", "Small sizes, tight stops."],
        "grumpy":    ["Let's just make the money back. 😤", "Sticking to the plan."],
        "neutral":   ["That's my 2 satoshis! 🪙", "Let's see how it plays out! 🤞",
                      "— Your friendly trading bot 🤖", "Catch you later! 👊"],
    }
    return random.choice(signoffs.get(mood, signoffs["neutral"]))


def _explanation_headline(asset: str, direction: str, confidence: float, mood: str, name: str) -> List[str]:
    parts = [_mood_greeting(mood, name)]
    if direction in ("BUY", "UP"):
        phrase = random.choice(_BULLISH_PHRASES)
        parts.append(f"\n{_conf_emoji(confidence)} *{asset}* is {phrase}!")
    elif direction in ("SELL", "DOWN"):
        phrase = random.choice(_BEARISH_PHRASES)
        parts.append(f"\n{_conf_emoji(confidence)} *{asset}* is {phrase}.")
    else:
        parts.append(f"\n⚖️ *{asset}* — no clear edge right now.")
    parts.append(_confidence_line(confidence, mood))
    return parts


def _explanation_trade_levels(entry: float, sl: float, tp: float, rr: Any) -> List[str]:
    parts: List[str] = []
    if entry:
        parts.append(f"\n💰 *Price:* `{entry:.5f}`")
        if sl:
            parts.append(f"🛑 *Stop:*  `{sl:.5f}`")
        if tp:
            parts.append(f"🎯 *Target:*`{tp:.5f}`")
        if rr:
            parts.append(f"📐 *R:R:* {float(rr):.2f}:1")
    return parts


def _explanation_context_line(regime: str, sess: str) -> str:
    context_parts = []
    if regime and regime not in ("unknown", ""):
        context_parts.append(f"Regime: {regime.replace('_', ' ')}")
    if sess:
        context_parts.append(f"Session: {sess}")
    if context_parts:
        return "  \n_" + " · ".join(context_parts) + "_"
    return ""


def _explanation_news_lines(news: Optional[List[Dict]]) -> List[str]:
    if not news:
        return []
    parts = ["\n📰 *In the news:*"]
    for article in news[:2]:
        headline = str(article.get("title", ""))[:80]
        if headline:
            parts.append(f"  • \"{headline}\"")
    return parts


def _technical_rsi_reasons(df: pd.DataFrame, latest, prev) -> List[str]:
    reasons: List[str] = []
    if "rsi" in df.columns:
        rsi = float(latest["rsi"])
        prev_rsi = float(prev["rsi"])
        if rsi < 30:
            reasons.append(f"RSI is *oversold* at {rsi:.1f} — bounce territory 📉")
        elif rsi < 40:
            reasons.append(f"RSI recovering from {prev_rsi:.1f} → {rsi:.1f}")
        elif rsi > 70:
            reasons.append(f"RSI is *overbought* at {rsi:.1f} — pullback risk 📈")
        elif rsi > 60:
            reasons.append(f"RSI showing strength at {rsi:.1f}")
        if abs(rsi - prev_rsi) > 10:
            moved = "jumped" if rsi > prev_rsi else "dropped"
            reasons.append(f"RSI just {moved} {abs(rsi - prev_rsi):.0f} points — momentum shift")
    return reasons


def _technical_volume_reasons(df: pd.DataFrame, latest) -> List[str]:
    reasons: List[str] = []
    if "volume" in df.columns and df["volume"].sum() > 0:
        cur_vol = float(latest["volume"])
        avg_vol = float(df["volume"].rolling(20).mean().iloc[-1])
        if avg_vol > 0:
            ratio = cur_vol / avg_vol
            if ratio > 2.5:
                reasons.append(f"Volume is *MASSIVE* ({ratio:.1f}x normal) — big money moving 🐋")
            elif ratio > 1.8:
                reasons.append(f"Strong volume ({ratio:.1f}x average) — confirms the move")
            elif ratio > 1.3:
                reasons.append(f"Above-average volume ({ratio:.1f}x normal)")
    return reasons


def _technical_trend_reasons(df: pd.DataFrame, latest, prev) -> List[str]:
    reasons: List[str] = []
    if "sma_20" in df.columns and "sma_50" in df.columns:
        price = float(latest["close"])
        s20 = float(latest["sma_20"])
        s50 = float(latest["sma_50"])
        if price > s20 > s50:
            reasons.append("Price above both 20 & 50 MAs — bulls in control ✅")
        elif price < s20 < s50:
            reasons.append("Price below both MAs — bears have the upper hand ⚠️")

    if "macd" in df.columns and "macd_signal" in df.columns:
        macd = float(latest["macd"])
        macd_sig = float(latest["macd_signal"])
        prev_macd = float(prev["macd"])
        prev_sig = float(prev["macd_signal"])
        if macd > macd_sig and prev_macd <= prev_sig:
            reasons.append("MACD just crossed bullish — fresh momentum 📈")
        elif macd < macd_sig and prev_macd >= prev_sig:
            reasons.append("MACD just crossed bearish — momentum turning 📉")

    if "adx" in df.columns:
        adx = float(latest["adx"])
        if adx > 40:
            reasons.append(f"Strong trend (ADX {adx:.0f}) — momentum is real 💪")
        elif adx > 25:
            reasons.append(f"Trend developing (ADX {adx:.0f})")
        elif adx < 15:
            reasons.append("Market is ranging — low trend strength ↔️")
    return reasons


def _technical_meta_reasons(signal: Dict) -> List[str]:
    reasons: List[str] = []
    meta = signal.get("metadata", {}) or {}
    if meta.get("sentiment_score"):
        s = float(meta["sentiment_score"])
        if s > 0.3:
            reasons.append(f"Sentiment is bullish ({s:+.2f}) — news flow positive")
        elif s < -0.3:
            reasons.append(f"Sentiment is bearish ({s:+.2f}) — negative news flow")
    if meta.get("whale_alert"):
        reasons.append("Whale activity detected — large money moving 🐋")
    return reasons


def _technical_reasons(df: pd.DataFrame, signal: Dict) -> List[str]:
    reasons: List[str] = []
    try:
        latest   = df.iloc[-1]
        prev     = df.iloc[-2] if len(df) > 1 else latest
        reasons.extend(_technical_rsi_reasons(df, latest, prev))
        reasons.extend(_technical_volume_reasons(df, latest))
        reasons.extend(_technical_trend_reasons(df, latest, prev))
        reasons.extend(_technical_meta_reasons(signal))

    except Exception as e:
        logger.debug(f"[Personality] _technical_reasons error: {e}")

    return reasons


def _fmt_asset_price(price: Any) -> str:
    try:
        value = float(price or 0.0)
    except Exception:
        return "n/a"
    if value == 0.0:
        return "n/a"
    if abs(value) >= 1000:
        return f"{value:,.2f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    if abs(value) >= 0.1:
        return f"{value:.4f}"
    return f"{value:.5f}"


def _humanize_runtime_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("_", " ").replace("-", " ").replace("/", " / ")
    text = " ".join(text.split())
    return text


def _humanize_runtime_reason(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "the setup is not ready yet"
    canned = {
        "no_playbook_seed": "the chart has not produced a playbook-quality seed yet",
        "market_closed": "the market is closed right now",
        "weekend_closed": "the market is closed for the weekend",
        "pullback_missing:trend_pullback": "the bot wants a proper pullback before entering",
        "trend_misaligned:aggressive_expansion": "the aggressive expansion idea is fighting the current structure",
        "trend_misaligned:intermarket_continuation": "the continuation idea is fighting the current structure",
        "alignment_too_weak:breakout_retest": "the breakout retest does not have enough structural alignment yet",
        "upside_exhausted:intermarket_continuation": "the move already looks stretched on the upside",
        "downside_exhausted:intermarket_continuation": "the move already looks stretched on the downside",
        "pattern family ranks below elite threshold": "the setup family still ranks below the execution threshold",
    }
    if raw in canned:
        return canned[raw]
    if ":" in raw:
        left, right = raw.split(":", 1)
        return f"{_humanize_runtime_label(left)} — {_humanize_runtime_label(right)}".strip(" -")
    return _humanize_runtime_label(raw)


def _analysis_bias_word(structure: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    signal = analysis.get("signal") if isinstance(analysis.get("signal"), dict) else {}
    direction = str(signal.get("direction") or analysis.get("playbook_decision", {}).get("direction") or "").upper()
    bias = str(structure.get("structure_bias") or "").lower()
    if bias == "buy" or direction == "BUY":
        return "bullish"
    if bias == "sell" or direction == "SELL":
        return "bearish"
    return "neutral"


def _analysis_setup_sentence(structure: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    bias_word = _analysis_bias_word(structure, analysis)
    breakout_ready = bool(structure.get("breakout_retest_ready"))
    pullback_ready = bool(structure.get("first_pullback_ready"))
    reclaim_ready = bool(structure.get("failed_opposite_move_confirmed"))
    breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
    pullback_score = float(structure.get("pullback_score", 0.0) or 0.0)
    pattern_family = _humanize_runtime_label(structure.get("pattern_family"))
    if breakout_ready:
        return f"structure is {bias_word} and a breakout retest is already on the board"
    if pullback_ready:
        return f"structure is {bias_word} and the first pullback is ready to be judged"
    if reclaim_ready:
        return f"structure is trying to build a reclaim-style reversal"
    if bias_word == "bullish":
        if breakout_score >= 0.20:
            return "structure still leans higher, but the breakout has not confirmed cleanly enough yet"
        if pullback_score >= 0.20:
            return "structure still leans higher, but the pullback has not reset cleanly enough yet"
        return "structure leans higher, but the chart is not giving the bot a clean trigger yet"
    if bias_word == "bearish":
        if breakout_score <= -0.20:
            return "structure still leans lower, but the breakdown has not confirmed cleanly enough yet"
        if pullback_score <= -0.20:
            return "structure still leans lower, but the bounce into sell territory is not clean enough yet"
        return "structure leans lower, but the chart is not giving the bot a clean trigger yet"
    if pattern_family:
        return f"the market is behaving more like {pattern_family.lower()} than a clean trend continuation"
    return "the market is still mixed and rangey"


def _analysis_level_sentence(structure: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    support_levels = list(structure.get("support_levels") or [])
    resistance_levels = list(structure.get("resistance_levels") or [])
    support = support_levels[0] if support_levels else structure.get("support")
    resistance = resistance_levels[0] if resistance_levels else structure.get("resistance")
    support_text = _fmt_asset_price(support)
    resistance_text = _fmt_asset_price(resistance)
    dist_to_support = structure.get("distance_to_support")
    dist_to_resistance = structure.get("distance_to_resistance")
    bias_word = _analysis_bias_word(structure, analysis)

    pieces: List[str] = []
    if support_text != "n/a" and resistance_text != "n/a":
        pieces.append(f"support is near {support_text} and resistance is near {resistance_text}")
    elif support_text != "n/a":
        pieces.append(f"nearest support is around {support_text}")
    elif resistance_text != "n/a":
        pieces.append(f"nearest resistance is around {resistance_text}")

    try:
        if bias_word == "bullish" and dist_to_resistance is not None and float(dist_to_resistance) <= 0.0035:
            pieces.append("price is already close to nearby resistance, so chasing here is lower quality")
        elif bias_word == "bearish" and dist_to_support is not None and float(dist_to_support) <= 0.0035:
            pieces.append("price is already close to nearby support, so chasing here is lower quality")
    except Exception:
        pass

    exhaustion = float(structure.get("dominant_exhaustion_score", 0.0) or 0.0)
    if exhaustion >= 0.58 or bool(structure.get("bias_exhausted")):
        pieces.append("the move is already stretched, so the bot does not want to force the entry")

    if not pieces:
        return ""
    return "Right now " + "; ".join(pieces) + "."


def _analysis_tape_sentence(df: Optional[pd.DataFrame], analysis: Dict[str, Any]) -> str:
    if df is None or df.empty:
        return ""
    signal = analysis.get("signal") if isinstance(analysis.get("signal"), dict) else {}
    reasons = _technical_reasons(df, signal or {"metadata": {"sentiment_score": analysis.get("sentiment_score", 0.0)}})
    clean_reasons = [str(item).strip() for item in reasons if str(item).strip()]
    if not clean_reasons:
        return ""
    return "On the tape: " + "; ".join(clean_reasons[:2]) + "."


def _analysis_flow_sentence(analysis: Dict[str, Any]) -> str:
    sentiment_score = float(analysis.get("sentiment_score", 0.0) or 0.0)
    cross = analysis.get("cross_asset_context") if isinstance(analysis.get("cross_asset_context"), dict) else {}
    market_intelligence = analysis.get("market_intelligence") if isinstance(analysis.get("market_intelligence"), dict) else {}
    narrative = _humanize_runtime_label(market_intelligence.get("dominant_narrative") or market_intelligence.get("narrative") or "")
    peer = str(cross.get("dominant_peer") or "").strip()
    cross_state = _humanize_runtime_label(cross.get("state") or "")

    parts: List[str] = []
    if sentiment_score >= 0.20:
        parts.append(f"background sentiment is leaning bullish at {sentiment_score:+.2f}")
    elif sentiment_score <= -0.20:
        parts.append(f"background sentiment is leaning bearish at {sentiment_score:+.2f}")
    if peer and cross_state:
        parts.append(f"{peer} is currently {cross_state.lower()}")
    if narrative:
        parts.append(f"the dominant narrative is {narrative.lower()}")
    if not parts:
        return ""
    return "Flow context: " + "; ".join(parts) + "."


def _analysis_wait_sentence(structure: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    market_status = analysis.get("market_status") if isinstance(analysis.get("market_status"), dict) else {}
    if not bool(market_status.get("market_open", False)):
        return "the next thing that matters is the reopen; after that the bot wants to see whether this structure still holds under live liquidity"
    if bool(structure.get("breakout_retest_ready")):
        return "the bot wants the retest to hold cleanly before it commits"
    if bool(structure.get("first_pullback_ready")):
        return "the bot wants the first pullback to hold and rotate back with conviction"
    bias_word = _analysis_bias_word(structure, analysis)
    if bias_word == "bullish":
        return "the bot wants either a cleaner pullback hold or a much cleaner breakout through resistance before buying"
    if bias_word == "bearish":
        return "the bot wants either a cleaner bounce into sell territory or a cleaner break through support before selling"
    return "the bot wants cleaner directional structure before it commits"


def _analysis_bot_paragraph(asset: str, analysis: Dict[str, Any], *, topic: str = "general") -> str:
    position = analysis.get("open_position") if isinstance(analysis.get("open_position"), dict) else {}
    structure = analysis.get("market_structure") if isinstance(analysis.get("market_structure"), dict) else {}
    signal = analysis.get("signal") if isinstance(analysis.get("signal"), dict) else {}
    playbook = analysis.get("playbook_decision") if isinstance(analysis.get("playbook_decision"), dict) else {}
    decision_status = str(analysis.get("decision_status") or "")
    decision_reason = _humanize_runtime_reason(analysis.get("decision_reason"))

    if position:
        direction = str(position.get("direction") or position.get("signal") or "").upper() or "BUY"
        pnl = position.get("pnl")
        pnl_text = ""
        try:
            pnl_text = f" Floating P&L is ${float(pnl):+.2f}." if pnl is not None else ""
        except Exception:
            pnl_text = ""
        return (
            f"The bot is already in a live {direction.lower()} on {asset} from {_fmt_asset_price(position.get('entry_price'))}."
            f"{pnl_text} It is managing that position first, so it is not looking for a second fresh entry on the same asset."
        )

    if signal and bool(signal.get("alive", True)):
        direction = str(signal.get("direction") or "").upper() or "BUY"
        confidence = float(signal.get("confidence", 0.0) or 0.0) * 100.0
        entry = _fmt_asset_price(signal.get("entry_price"))
        stop = _fmt_asset_price(signal.get("stop_loss"))
        target = _fmt_asset_price(signal.get("take_profit"))
        playbook = analysis.get("playbook_decision") if isinstance(analysis.get("playbook_decision"), dict) else {}
        meta = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        playbook_name = _humanize_runtime_label(playbook.get("playbook") or meta.get("playbook_name"))
        entry_style = _humanize_runtime_label(playbook.get("entry_style") or meta.get("playbook_entry_style"))
        session_label = _humanize_runtime_label(playbook.get("session_label") or meta.get("session_label") or meta.get("playbook_session"))
        broker = analysis.get("broker_quality") if isinstance(analysis.get("broker_quality"), dict) else {}
        broker_bits = [
            _humanize_runtime_label(broker.get("primary_provider")),
            _humanize_runtime_label(broker.get("quote_quality_state")),
            _humanize_runtime_label(broker.get("spread_regime")),
        ]
        broker_bits = [bit for bit in broker_bits if bit]
        context_bits = []
        if playbook_name:
            context = playbook_name.lower()
            if entry_style:
                context += f" / {entry_style.lower()}"
            context_bits.append(context)
        if session_label:
            context_bits.append(f"session {session_label.lower()}")
        if broker_bits:
            context_bits.append("quotes " + ", ".join(bit.lower() for bit in broker_bits[:3]))
        context_prefix = ""
        if context_bits:
            context_prefix = "The active idea is " + "; ".join(context_bits[:3]) + ". "
        return (
            f"{context_prefix}Bot posture: it is willing to {direction.lower()} here, with a working plan around {entry},"
            f" stop {stop}, target {target}, and about {confidence:.0f}% live conviction."
        )

    if decision_status == "killed":
        blocks = []
        if signal:
            meta = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
            blocks = list(meta.get("execution_hard_blocks") or meta.get("late_entry_risk_reasons") or [])
        block_text = "; ".join(_humanize_runtime_reason(item) for item in blocks[:2]) if blocks else decision_reason
        wait_text = _analysis_wait_sentence(structure, analysis)
        return (
            f"The bot had a directional idea, but it killed the entry because {block_text}. "
            f"What it wants next is simple: {wait_text}."
        )

    if topic == "confidence":
        playbook_conf = float(playbook.get("confidence", 0.0) or 0.0) * 100.0
        align = float(structure.get("alignment_score", 0.0) or 0.0) * 100.0
        setup = float(structure.get("setup_quality", 0.0) or 0.0) * 100.0
        return (
            f"There is no live execution confidence to quote yet. Structurally the chart is running around "
            f"{align:.0f}% alignment and {setup:.0f}% setup quality, with playbook conviction near {playbook_conf:.0f}% when a candidate exists."
        )

    if topic == "risk":
        return (
            f"There is no live entry plan yet, so there is no active stop or target on {asset}. "
            f"The real risk right now is forcing an entry before {_analysis_wait_sentence(structure, analysis)}."
        )

    if topic == "sentiment":
        flow = _analysis_flow_sentence(analysis)
        if flow:
            return flow
        return f"There is no meaningful sentiment edge on {asset} right now; the structure matters more than social or flow inputs here."

    wait_text = _analysis_wait_sentence(structure, analysis)
    return f"The bot is flat here for now. What it is waiting for next is {wait_text}."


def _analysis_market_paragraph(asset: str, analysis: Dict[str, Any]) -> str:
    market_status = analysis.get("market_status") if isinstance(analysis.get("market_status"), dict) else {}
    market_open = bool(market_status.get("market_open", False))
    market_reason = _humanize_runtime_reason(market_status.get("reason"))
    current_price = _fmt_asset_price(analysis.get("current_price") or analysis.get("latest_close"))
    category = str(analysis.get("category") or "").lower()
    if market_open:
        return f"{asset} is currently tradable, and the latest tracked price is around {current_price}."
    closed_reason = market_reason or "the session is shut"
    if category != "crypto":
        return (
            f"{asset} is in watch mode right now because {closed_reason}. "
            f"The bot can still read the structure, but it will not call this a live executable setup until the market reopens."
        )
    return f"{asset} is trading around {current_price}, but the bot is not seeing a live executable edge yet."


def _analysis_structure_paragraph(asset: str, analysis: Dict[str, Any], df: Optional[pd.DataFrame]) -> str:
    structure = analysis.get("market_structure") if isinstance(analysis.get("market_structure"), dict) else {}
    if not structure:
        return f"The current chart state on {asset} is not fully available yet."
    setup_sentence = _analysis_setup_sentence(structure, analysis)
    level_sentence = _analysis_level_sentence(structure, analysis)
    tape_sentence = _analysis_tape_sentence(df, analysis)
    parts = [item for item in [setup_sentence[:1].upper() + setup_sentence[1:] + ".", level_sentence, tape_sentence] if item]
    return " ".join(parts)


def _analysis_memory_paragraph(asset: str, memory: Dict[str, Any]) -> str:
    if not memory.get("has_memory"):
        return ""
    total_trades = int(memory.get("total_trades", 0) or 0)
    if total_trades <= 0:
        return ""
    return (
        f"Memory check: the bot has traded {asset} {total_trades} times recently, with "
        f"{memory.get('win_rate', 0)}% wins and ${float(memory.get('total_pnl', 0.0) or 0.0):+.2f} total P&L."
    )


def _render_market_state_response(
    asset: str,
    analysis: Dict[str, Any],
    df: Optional[pd.DataFrame],
    mood: str,
    memory: Dict[str, Any],
    *,
    topic: str = "general",
) -> str:
    analysis = analysis or {}
    market_status = analysis.get("market_status") if isinstance(analysis.get("market_status"), dict) else {}
    market_open = bool(market_status.get("market_open", False))
    decision_status = str(analysis.get("decision_status") or "")
    signal = analysis.get("signal") if isinstance(analysis.get("signal"), dict) else {}
    position = analysis.get("open_position") if isinstance(analysis.get("open_position"), dict) else {}

    if position:
        headline = f"*{asset}* — already in a live trade."
    elif signal and bool(signal.get("alive", True)):
        direction = str(signal.get("direction") or "").upper() or "BUY"
        headline = f"*{asset}* — live {direction.lower()} setup."
    elif decision_status == "killed":
        headline = f"*{asset}* — setup spotted, but entry is blocked."
    elif not market_open:
        headline = f"*{asset}* — market closed, watch mode."
    else:
        headline = f"*{asset}* — no live entry yet, but the chart is still active."

    paragraphs = [headline, _analysis_market_paragraph(asset, analysis), _analysis_structure_paragraph(asset, analysis, df)]
    flow_paragraph = _analysis_flow_sentence(analysis)
    if flow_paragraph and topic in {"why", "trade", "sentiment", "general", "signal"}:
        paragraphs.append(flow_paragraph)
    paragraphs.append(_analysis_bot_paragraph(asset, analysis, topic=topic))
    memory_paragraph = _analysis_memory_paragraph(asset, memory)
    if memory_paragraph and topic in {"why", "trade", "general", "signal"}:
        paragraphs.append(memory_paragraph)

    if mood in {"grumpy", "cautious", "shaken"} and topic in {"trade", "why", "general"}:
        paragraphs.append("Because recent form has been choppier, the bot is leaning toward cleaner price location and better confirmation rather than forcing the first idea it sees.")

    return "\n\n".join(part for part in paragraphs if str(part or "").strip())


_BULLISH_PHRASES = [
    "looking bullish", "ready to pump", "showing strength",
    "on the move", "looking juicy", "getting interesting",
    "buyers are stepping in", "breaking out", "building momentum",
]
_BEARISH_PHRASES = [
    "looking bearish", "showing weakness", "might dip",
    "under pressure", "sellers are in control", "looking shaky",
    "rolling over", "losing momentum", "distribution happening",
]
