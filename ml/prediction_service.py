from __future__ import annotations
import json
import socket
import struct
import threading
import time
from typing import Optional, Tuple
from config.config import ML_SERVICE_PORT
from utils.logger import get_logger

logger = get_logger()

_HOST        = "127.0.0.1"
_PORT        = ML_SERVICE_PORT
_TIMEOUT_SEC = 2.0
_HEADER_FMT  = "!I"   # 4-byte big-endian unsigned int (message length)
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)


def _send_msg(sock, payload: dict) -> None:
    data = json.dumps(payload).encode()
    sock.sendall(struct.pack(_HEADER_FMT, len(data)) + data)


def _recv_msg(sock) -> Optional[dict]:
    header = _recv_exact(sock, _HEADER_SIZE)
    if not header:
        return None
    length = struct.unpack(_HEADER_FMT, header)[0]
    body   = _recv_exact(sock, length)
    return json.loads(body.decode()) if body else None


def _recv_exact(sock, n: int) -> Optional[bytes]:
    # FIX: The original loop had no timeout on individual recv() calls.
    # If the server sent fewer bytes than expected (e.g. connection reset
    # mid-message) the client would hang indefinitely, blocking the trading
    # loop.  We rely on the socket's own timeout (set via settimeout() or
    # create_connection(..., timeout=...)) to bound each recv() call.
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf


# ── Server (runs in its own process) ──────────────────────────────────────────

class PredictionServer:
    """
    Serves ML predictions over a local TCP socket.
    One predictor instance is shared across all client connections.
    """

    def __init__(self, host: str = _HOST, port: int = _PORT):
        self._host      = host
        self._port      = port
        self._predictor = None
        self._registry  = None

    def _init_predictor(self):
        # FIX: Import MLPredictor instead of Predictor
        from ml.predictor import MLPredictor
        from ml.registry  import registry
        self._predictor = MLPredictor()
        self._registry  = registry
        self._registry.load_all()
        logger.info(f"[MLService] Predictor ready on {self._host}:{self._port}")

    def _handle_client(self, conn, addr):
        try:
            while True:
                msg = _recv_msg(conn)
                if msg is None:
                    break
                action = msg.get("action")

                if action == "predict":
                    import pandas as pd
                    df       = pd.DataFrame(msg["ohlcv"])
                    category = msg.get("category", "crypto")
                    asset    = msg.get("asset", "")
                    try:
                        # MLPredictor.predict() returns (probability, confidence)
                        # Convert to (direction, probability) for client
                        prob, conf = self._predictor.predict(asset, category, df)
                        direction = "BUY" if prob > 0.5 else "SELL" if prob < 0.5 else "HOLD"
                        _send_msg(conn, {
                            "direction":   direction,
                            "probability": float(prob),
                            "confidence":  float(conf),
                            "ok": True,
                        })
                    except Exception as e:
                        _send_msg(conn, {"ok": False, "error": str(e)})

                elif action == "health":
                    _send_msg(conn, {"ok": True, "uptime": time.time()})

                else:
                    _send_msg(conn, {"ok": False, "error": f"Unknown action: {action}"})
        except Exception as e:
            logger.debug(f"[MLService] Client {addr} disconnected: {e}")
        finally:
            conn.close()

    def run(self):
        self._init_predictor()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self._host, self._port))
        srv.listen(10)
        logger.info(f"[MLService] Listening on {self._host}:{self._port}")
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(
                target=self._handle_client, args=(conn, addr), daemon=True
            )
            t.start()


# ── Client (used by trading engine) ───────────────────────────────────────────

class PredictionClient:
    """
    Drop-in replacement for ml.predictor.MLPredictor.
    Falls back to the local MLPredictor if the service is unreachable.
    """

    def __init__(self, host: str = _HOST, port: int = _PORT):
        self._host       = host
        self._port       = port
        self._fallback   = None
        self._lock       = threading.Lock()

    def _get_fallback(self):
        if self._fallback is None:
            # FIX: Import MLPredictor instead of Predictor
            from ml.predictor import MLPredictor
            self._fallback = MLPredictor()
        return self._fallback

    def predict_next(
        self,
        df,
        category: str = "crypto",
        asset: str = "",
    ) -> Tuple[str, float]:
        """Same signature as MLPredictor.predict_next()."""
        try:
            with socket.create_connection(
                (self._host, self._port), timeout=_TIMEOUT_SEC
            ) as conn:
                _send_msg(conn, {
                    "action":   "predict",
                    "ohlcv":    df.to_dict(orient="list"),
                    "category": category,
                    "asset":    asset,
                })
                resp = _recv_msg(conn)
                if resp and resp.get("ok"):
                    return resp["direction"], resp["probability"]
        except Exception as e:
            logger.debug(f"[MLClient] Service unreachable ({e}) — using local fallback")
        
        # Fallback: call MLPredictor
        prob, conf = self._get_fallback().predict(asset, category, df)
        direction = "BUY" if prob > 0.5 else "SELL" if prob < 0.5 else "HOLD"
        return direction, float(prob)

    def predict(self, asset: str, category: str, df) -> Tuple[float, float]:
        """Wrapper for MLPredictor.predict() signature expected by TradingCore."""
        try:
            with socket.create_connection(
                (self._host, self._port), timeout=_TIMEOUT_SEC
            ) as conn:
                _send_msg(conn, {
                    "action":   "predict",
                    "ohlcv":    df.to_dict(orient="list"),
                    "category": category,
                    "asset":    asset,
                })
                resp = _recv_msg(conn)
                if resp and resp.get("ok"):
                    prob = float(resp.get("probability", 0.5))
                    conf = abs(prob - 0.5) * 2  # Convert to 0-1 confidence
                    return prob, conf
        except Exception as e:
            logger.debug(f"[MLClient] Service unreachable ({e}) — using local fallback")
        
        # Fallback: call MLPredictor.predict() directly
        return self._get_fallback().predict(asset, category, df)


# ── Standalone entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    PredictionServer().run()
