"""
services/personality_service.py — Robbie's personality, memory, and signal explanation engine.

Robbie is the bot's human persona. He:
  - Remembers every trade in a diary (TradingDiary table)
  - Has moods that shift based on recent P&L (BotPersonality table)
  - Records memorable wins and losses (MemorableMoments table)
  - Explains signals in plain English with context from his history
  - Answers free-form questions via /ask

Wiring (called from core/engine.py on_trade_closed):
    from services.personality_service import personality
    personality.record_trade(trade_dict)

/ask, /mood, /diary all call PersonalityDatabase directly.
"""

from __future__ import annotations

import random
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from config.database import SessionLocal
from models.trade_models import (
    BotPersonality, HumanExplanations,
    MemorableMoments, Trade, TradingDiary,
)
from utils.logger import logger


# ══════════════════════════════════════════════════════════════════════════════
# PersonalityDatabase — all DB reads and writes for the personality system
# ══════════════════════════════════════════════════════════════════════════════

class PersonalityDatabase:
    """Thread-safe wrapper around the personality DB tables."""

    def __init__(self):
        self.session = SessionLocal()
        self._lock   = threading.Lock()
        self._ensure_personality_exists()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _ensure_personality_exists(self) -> None:
        """Create Robbie's default personality row if none exists."""
        try:
            p = self.session.query(BotPersonality).first()
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
                self.session.add(p)
                self.session.commit()
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
                # Classify setup type from signal metadata
                meta       = trade_data.get("metadata", trade_data.get("trade_metadata", {})) or {}
                regime     = str(meta.get("regime", "unknown")).lower()
                rsi        = float(meta.get("rsi", 0) or 0)
                setup_type = _classify_setup(regime, rsi, trade_data.get("exit_reason", ""))

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
                    },
                )
                self.session.add(entry)
                self.session.commit()

                self._update_mood_from_trade(trade_data)
                self._check_memorable(trade_data, setup_type)

                return entry.id

            except Exception as e:
                logger.error(f"[Personality] record_trade failed: {e}")
                try:
                    self.session.rollback()
                except Exception:
                    pass
                return None

    def _update_mood_from_trade(self, trade_data: Dict) -> None:
        """Shift Robbie's mood based on the closed trade."""
        try:
            p    = self.session.query(BotPersonality).first()
            if not p:
                return
            pnl  = float(trade_data.get("pnl", 0))
            win  = pnl > 0

            if win:
                p.consecutive_wins   += 1
                p.consecutive_losses  = 0
            else:
                p.consecutive_losses += 1
                p.consecutive_wins    = 0

            p.total_trades_remembered += 1

            # Recalculate last-10 stats from DB
            recent = (
                self.session.query(TradingDiary)
                .order_by(TradingDiary.created_at.desc())
                .limit(10).all()
            )
            if recent:
                p.last_10_wins = sum(1 for t in recent if t.pnl and t.pnl > 0)
                p.last_10_pnl  = sum(float(t.pnl) for t in recent if t.pnl)

            p.current_mood, p.mood_emoji = _calculate_mood(p)
            self.session.commit()
        except Exception as e:
            logger.debug(f"[Personality] mood update failed: {e}")

    def _check_memorable(self, trade_data: Dict, setup_type: str) -> None:
        """Save a MemorableMoment if this trade crosses a significance threshold."""
        try:
            pnl    = float(trade_data.get("pnl", 0))
            asset  = trade_data.get("asset", "")
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
            self.session.add(moment)
            self.session.commit()
        except Exception as e:
            logger.debug(f"[Personality] memorable check failed: {e}")

    # ── Historical context ────────────────────────────────────────────────────

    def find_similar_setups(self, asset: str, setup_type: str,
                             days_back: int = 30) -> List[Dict]:
        """Return past diary entries for the same asset+setup."""
        try:
            cutoff  = datetime.now() - timedelta(days=days_back)
            rows    = (
                self.session.query(TradingDiary)
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
            rows   = (
                self.session.query(TradingDiary)
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
            p = self.session.query(BotPersonality).first()

            last_week    = datetime.now() - timedelta(days=7)
            weekly_total = self.session.query(TradingDiary).filter(
                TradingDiary.created_at >= last_week
            ).count()
            weekly_wins  = self.session.query(TradingDiary).filter(
                TradingDiary.created_at >= last_week,
                TradingDiary.pnl > 0,
            ).count()

            moments = (
                self.session.query(MemorableMoments)
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
            row  = HumanExplanations(
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
            self.session.add(row)
            self.session.commit()
            return row.id
        except Exception as e:
            logger.debug(f"[Personality] save_explanation failed: {e}")
            return None

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass


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
    ) -> str:
        """
        Generate Robbie's full signal explanation.
        Called by /why command and the dashboard overlay.
        """
        report    = self.db.get_personality_report()
        mood      = report["current_mood"]
        direction = signal.get("direction", signal.get("signal", "HOLD"))
        confidence= float(signal.get("confidence", 0.5)) * 100
        entry     = float(signal.get("entry_price", 0))
        sl        = float(signal.get("stop_loss", 0))
        tp        = float(signal.get("take_profit", 0))
        meta      = signal.get("metadata", {}) or {}
        regime    = meta.get("regime", "unknown")
        sess      = meta.get("session", "")
        rr        = signal.get("risk_reward", signal.get("rr_ratio", 0))

        parts = []

        # ── Greeting (mood-driven) ───────────────────────────────────────────
        greeting = _mood_greeting(mood, report["name"])
        parts.append(greeting)

        # ── Main call ────────────────────────────────────────────────────────
        if direction in ("BUY", "UP"):
            phrase = random.choice(_BULLISH_PHRASES)
            parts.append(f"\n{_conf_emoji(confidence)} *{asset}* is {phrase}!")
        elif direction in ("SELL", "DOWN"):
            phrase = random.choice(_BEARISH_PHRASES)
            parts.append(f"\n{_conf_emoji(confidence)} *{asset}* is {phrase}.")
        else:
            parts.append(f"\n⚖️ *{asset}* — no clear edge right now.")

        # ── Confidence line ──────────────────────────────────────────────────
        parts.append(_confidence_line(confidence, mood))

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
        if entry:
            parts.append(f"\n💰 *Price:* `{entry:.5f}`")
            if sl:   parts.append(f"🛑 *Stop:*  `{sl:.5f}`")
            if tp:   parts.append(f"🎯 *Target:*`{tp:.5f}`")
            if rr:   parts.append(f"📐 *R:R:* {float(rr):.2f}:1")

        # ── Context ──────────────────────────────────────────────────────────
        context_parts = []
        if regime and regime not in ("unknown", ""):
            context_parts.append(f"Regime: {regime.replace('_', ' ')}")
        if sess:
            context_parts.append(f"Session: {sess}")
        if context_parts:
            parts.append("  \n_" + " · ".join(context_parts) + "_")

        # ── News ─────────────────────────────────────────────────────────────
        if news:
            parts.append("\n📰 *In the news:*")
            for article in news[:2]:
                headline = str(article.get("title", ""))[:80]
                if headline:
                    parts.append(f"  • \"{headline}\"")

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
    ) -> str:
        """
        Answer a free-form question about an asset in Robbie's voice.
        Called by the /ask Telegram command.
        """
        report  = self.db.get_personality_report()
        mood    = report["current_mood"]
        memory  = self.db.get_asset_memory(asset)
        q_lower = question.lower()

        # ── Route to the right answer type ───────────────────────────────────

        if any(w in q_lower for w in ("buy", "sell", "should i", "enter", "trade")):
            return self._answer_trade_question(asset, question, signal, df, mood, memory)

        if any(w in q_lower for w in ("remember", "history", "last time", "before", "previously", "diary")):
            return self._answer_memory_question(asset, mood, memory)

        if any(w in q_lower for w in ("feel", "mood", "confident", "nervous", "worried")):
            return self._answer_mood_question(asset, mood, report)

        if any(w in q_lower for w in ("why", "reason", "explain", "because")):
            if signal and df is not None:
                return self.explain_signal(asset, df, signal)
            return self._answer_why_no_signal(asset, mood)

        if any(w in q_lower for w in ("risk", "stop", "loss", "safe")):
            return self._answer_risk_question(asset, signal, mood, memory)

        if any(w in q_lower for w in ("news", "sentiment", "social", "twitter")):
            return self._answer_sentiment_question(asset, mood)

        # Default — general take
        return self._answer_general(asset, question, signal, mood, memory)

    def _answer_trade_question(self, asset, question, signal, df, mood, memory) -> str:
        if not signal or signal.get("direction", "HOLD") == "HOLD":
            no_sig = random.choice([
                f"Honestly? I'm not seeing a clean entry on {asset} right now. 🤷",
                f"The pipeline isn't giving me a clear signal on {asset} at the moment.",
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
                f"The pipeline flagged {asset} {direction.lower()}.",
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
            f"Run `/signal {asset}` to check the live pipeline, then `/ask {asset} why` once a signal is active."
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

    def _answer_sentiment_question(self, asset, mood) -> str:
        return (
            f"I don't have a live news feed wired into this command right now. "
            f"For sentiment context, check `/why {asset}` which pulls from the "
            f"sentiment layer of the signal pipeline — that's where I factor in "
            f"news and social signals."
        )

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
            lines.append(f"My current read: *{d}* at {conf:.0f}% confidence.")

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
        return f"And I'm *very* confident about this one — {confidence:.0f}%. 🔥"
    if mood in ("cautious", "shaken") and confidence < 75:
        return f"Confidence is {confidence:.0f}% — I'd keep the size small given how this week has gone."
    if confidence > 80:
        return f"I'm *pretty damn confident* about this ({confidence:.0f}% sure)."
    if confidence > 65:
        return f"Decent signal here ({confidence:.0f}% confidence)."
    if confidence > 52:
        return f"Not super confident ({confidence:.0f}%) but worth watching."
    return f"Honestly a bit unsure ({confidence:.0f}%) — market's being weird."


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


def _technical_reasons(df: pd.DataFrame, signal: Dict) -> List[str]:
    reasons = []
    try:
        latest   = df.iloc[-1]
        prev     = df.iloc[-2] if len(df) > 1 else latest

        if "rsi" in df.columns:
            rsi      = float(latest["rsi"])
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

        if "volume" in df.columns and df["volume"].sum() > 0:
            cur_vol  = float(latest["volume"])
            avg_vol  = float(df["volume"].rolling(20).mean().iloc[-1])
            if avg_vol > 0:
                ratio = cur_vol / avg_vol
                if ratio > 2.5:
                    reasons.append(f"Volume is *MASSIVE* ({ratio:.1f}x normal) — big money moving 🐋")
                elif ratio > 1.8:
                    reasons.append(f"Strong volume ({ratio:.1f}x average) — confirms the move")
                elif ratio > 1.3:
                    reasons.append(f"Above-average volume ({ratio:.1f}x normal)")

        if "sma_20" in df.columns and "sma_50" in df.columns:
            price = float(latest["close"])
            s20   = float(latest["sma_20"])
            s50   = float(latest["sma_50"])
            if price > s20 > s50:
                reasons.append("Price above both 20 & 50 MAs — bulls in control ✅")
            elif price < s20 < s50:
                reasons.append("Price below both MAs — bears have the upper hand ⚠️")

        if "macd" in df.columns and "macd_signal" in df.columns:
            macd     = float(latest["macd"])
            macd_sig = float(latest["macd_signal"])
            prev_macd= float(prev["macd"])
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

        meta = signal.get("metadata", {}) or {}
        if meta.get("sentiment_score"):
            s = float(meta["sentiment_score"])
            if s > 0.3:
                reasons.append(f"Sentiment is bullish ({s:+.2f}) — news flow positive")
            elif s < -0.3:
                reasons.append(f"Sentiment is bearish ({s:+.2f}) — negative news flow")

        if meta.get("whale_alert"):
            reasons.append("Whale activity detected — large money moving 🐋")

    except Exception as e:
        logger.debug(f"[Personality] _technical_reasons error: {e}")

    return reasons


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