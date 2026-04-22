from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config.config import (
    DEEPSEEK_API_KEY,
    ROBBIE_CHAT_BASE_URL,
    ROBBIE_CHAT_HISTORY_LIMIT,
    ROBBIE_CHAT_MAX_TOKENS,
    ROBBIE_CHAT_MODEL,
    ROBBIE_CHAT_TEMPERATURE,
    ROBBIE_CHAT_TIMEOUT_SECONDS,
)
from utils.display_time import display_timezone_label, now_in_display_timezone
from utils.logger import get_logger

logger = get_logger()

_SESSION_FILE = Path("data/deepseek_chat_sessions.json")
_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
_MAX_HISTORY_MESSAGES = max(2, int(ROBBIE_CHAT_HISTORY_LIMIT or 10)) * 2


def _clip_text(text: Any, limit: int = 600) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def _json_text(value: Any, limit: int = 1600) -> str:
    try:
        text = json.dumps(value, ensure_ascii=True, default=str)
    except Exception:
        text = str(value)
    return _clip_text(text, limit)


class ChatSessionStore:
    def __init__(self, path: Path | str = _SESSION_FILE):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        with self._lock:
            if not self._path.exists():
                self._sessions = {}
                return
            try:
                payload = json.loads(self._path.read_text(encoding="utf-8"))
                sessions = payload.get("sessions") if isinstance(payload, dict) else {}
                self._sessions = sessions if isinstance(sessions, dict) else {}
            except Exception:
                self._sessions = {}

    def _persist(self) -> None:
        tmp = self._path.with_suffix(".tmp")
        payload = {
            "sessions": self._sessions,
            "updated_at": int(time.time()),
        }
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._path)

    def get(self, chat_id: str) -> Dict[str, Any]:
        with self._lock:
            key = str(chat_id)
            session = dict(self._sessions.get(key, {}) or {})
            session.setdefault("messages", [])
            session.setdefault("updated_at", 0)
            return session

    def append_turn(self, chat_id: str, *, user_message: str, assistant_message: str) -> None:
        with self._lock:
            key = str(chat_id)
            session = dict(self._sessions.get(key, {}) or {})
            messages = list(session.get("messages") or [])
            messages.append({"role": "user", "content": str(user_message or "")})
            messages.append({"role": "assistant", "content": str(assistant_message or "")})
            session["messages"] = messages[-_MAX_HISTORY_MESSAGES:]
            session["updated_at"] = int(time.time())
            self._sessions[key] = session
            self._persist()

    def reset(self, chat_id: str) -> None:
        with self._lock:
            self._sessions.pop(str(chat_id), None)
            self._persist()


class DeepSeekChatService:
    def __init__(self, *, session_store: Optional[ChatSessionStore] = None):
        self._sessions = session_store or ChatSessionStore()

    def reset(self, chat_id: str) -> None:
        self._sessions.reset(chat_id)

    def answer(self, *, question: str, chat_id: str) -> str:
        prompt = str(question or "").strip()
        if not prompt:
            return "Send me a message and I will answer it."

        session = self._sessions.get(chat_id)
        response = self._answer_via_deepseek(prompt, session)
        self._sessions.append_turn(
            chat_id,
            user_message=prompt,
            assistant_message=response,
        )
        return response

    def _system_prompt(self) -> str:
        local_now = now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}")
        return (
            "You are DeepSeek running as a dedicated private Telegram chat bot. "
            "This bot is intentionally separated from the trading bot, so you do not have access to live signals, positions, dashboards, order flow, P&L, or hidden runtime state unless the user explicitly pastes that information into the conversation. "
            "Answer naturally, directly, and without pretending to know live bot state. "
            f"Current local time is {local_now}. "
            f"Use {display_timezone_label()} for relative date references."
        )

    def _answer_via_deepseek(self, question: str, session: Dict[str, Any]) -> str:
        if not DEEPSEEK_API_KEY:
            raise RuntimeError("DEEPSEEK_API_KEY is not configured")

        base_url = str(ROBBIE_CHAT_BASE_URL or "https://api.deepseek.com").rstrip("/")
        endpoint = f"{base_url}/chat/completions"
        history = list(session.get("messages") or [])[-_MAX_HISTORY_MESSAGES:]
        messages = [
            {"role": "system", "content": self._system_prompt()},
            *history,
            {"role": "user", "content": question},
        ]
        payload = {
            "model": str(ROBBIE_CHAT_MODEL or "deepseek-chat"),
            "messages": messages,
            "temperature": max(0.0, min(1.1, float(ROBBIE_CHAT_TEMPERATURE or 0.35))),
            "max_tokens": max(300, min(2000, int(ROBBIE_CHAT_MAX_TOKENS or 1100))),
        }
        try:
            logger.debug(
                "[DeepSeekChat] sending prompt",
                extra={
                    "endpoint": endpoint,
                    "history_len": len(history),
                    "payload_preview": _json_text(payload, 1000),
                },
            )
        except Exception:
            pass

        resp = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=float(ROBBIE_CHAT_TIMEOUT_SECONDS or 20),
        )
        resp.raise_for_status()
        body = resp.json() if resp.content else {}
        choices = body.get("choices") if isinstance(body, dict) else []
        if not isinstance(choices, list) or not choices:
            return "DeepSeek returned no answer."
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        content = str((message or {}).get("content") or "").strip()
        return content or "DeepSeek returned an empty answer."


_deepseek_chat_service: Optional[DeepSeekChatService] = None
_deepseek_chat_service_lock = threading.Lock()


def get_deepseek_chat_service() -> DeepSeekChatService:
    global _deepseek_chat_service
    with _deepseek_chat_service_lock:
        if _deepseek_chat_service is None:
            _deepseek_chat_service = DeepSeekChatService()
        return _deepseek_chat_service


__all__ = ["ChatSessionStore", "DeepSeekChatService", "get_deepseek_chat_service"]
