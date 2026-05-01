#!/usr/bin/env bash
set -euo pipefail

echo "== Environment checks =="
if [[ ! -f .env ]]; then
  echo "FAIL: .env not found"
  exit 1
fi

development_mode="$(python3 - <<'PY'
from pathlib import Path
env = {}
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip()
print(env.get("DEVELOPMENT_MODE", ""))
PY
)"

dashboard_cors="$(python3 - <<'PY'
from pathlib import Path
env = {}
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip()
print(env.get("DASHBOARD_CORS_ORIGINS", ""))
PY
)"

dashboard_api_key="$(python3 - <<'PY'
from pathlib import Path
env = {}
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip()
print(env.get("DASHBOARD_API_KEY", ""))
PY
)"

database_url="$(python3 - <<'PY'
from pathlib import Path
env = {}
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip()
print(env.get("DATABASE_URL", ""))
PY
)"

printf "%-28s%s\n" "DEVELOPMENT_MODE" "${development_mode:-<unset>}"
printf "%-28s%s\n" "DASHBOARD_CORS_ORIGINS" "${dashboard_cors:-<unset>}"
printf "%-28s%s\n" "DASHBOARD_API_KEY" "$( [[ -n "${dashboard_api_key}" ]] && echo '<set>' || echo '<unset>' )"

if [[ "${development_mode,,}" != "false" ]]; then
  echo "WARN: DEVELOPMENT_MODE should be false in production"
fi

if [[ -z "${dashboard_api_key}" ]] || [[ "${dashboard_api_key}" == replace-* ]]; then
  echo "WARN: DASHBOARD_API_KEY is missing or still a placeholder"
fi

if [[ "${dashboard_cors}" == *"localhost"* ]] || [[ "${dashboard_cors}" == *"127.0.0.1"* ]]; then
  echo "WARN: DASHBOARD_CORS_ORIGINS still points at localhost"
fi

if [[ -z "${database_url}" ]] || [[ "${database_url}" == *"replace-password"* ]]; then
  echo "WARN: DATABASE_URL is missing or still a placeholder"
fi

required_keys=(
  "OPENAI_API_KEY"
  "DERIV_APP_ID"
  "DERIV_TOKEN"
  "COMMAND_BOT_TOKEN"
  "COMMAND_BOT_CHAT_ID"
  "WHALE_TELEGRAM_TOKEN"
  "INTELLIGENCE_CHAT_ID"
  "TELEGRAM_API_ID"
  "TELEGRAM_API_HASH"
  "TELEGRAM_PHONE"
)

echo
echo "== Core runtime keys =="
python3 - <<'PY'
from pathlib import Path
required = [
    "OPENAI_API_KEY",
    "DERIV_APP_ID",
    "DERIV_TOKEN",
    "COMMAND_BOT_TOKEN",
    "COMMAND_BOT_CHAT_ID",
    "WHALE_TELEGRAM_TOKEN",
    "INTELLIGENCE_CHAT_ID",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE",
]
env = {}
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip()
for key in required:
    value = env.get(key, "")
    state = "<set>" if value and not value.startswith("replace-") else "<missing>"
    print(f"{key:<28}{state}")
PY

missing_required="$(python3 - <<'PY'
from pathlib import Path
required = [
    "OPENAI_API_KEY",
    "DERIV_APP_ID",
    "DERIV_TOKEN",
    "COMMAND_BOT_TOKEN",
    "COMMAND_BOT_CHAT_ID",
    "WHALE_TELEGRAM_TOKEN",
    "INTELLIGENCE_CHAT_ID",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_PHONE",
]
env = {}
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip()
missing = [key for key in required if not env.get(key, "") or env.get(key, "").startswith("replace-")]
print(",".join(missing))
PY
)"

if [[ -n "${missing_required}" ]]; then
  echo "WARN: Missing core runtime keys: ${missing_required}"
fi

echo
echo "== Local service checks =="
ss -ltnp | grep -E ':(5000|8081|9100)\s' || true

if command -v systemctl >/dev/null 2>&1; then
  echo
  echo "== systemd service checks =="
  systemctl is-enabled forex-bot 2>/dev/null || true
  systemctl is-active forex-bot 2>/dev/null || true
  systemctl is-active nginx 2>/dev/null || true
fi

echo
echo "== HTTP status check =="
api_key="$(python3 - <<'PY'
from pathlib import Path
value = ""
env_path = Path(".env")
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DASHBOARD_API_KEY="):
            value = line.split("=", 1)[1].strip()
            break
print(value)
PY
)"

if [[ -n "${api_key}" ]]; then
  login_payload="$(printf '{"api_key":"%s"}' "${api_key}")"
  token="$(curl -fsS -X POST http://127.0.0.1:5000/api/login -H 'Content-Type: application/json' -d "${login_payload}" | python3 - <<'PY'
import json, sys
try:
    data = json.load(sys.stdin)
    print(data.get("token", ""))
except Exception:
    print("")
PY
)"
  if [[ -n "${token}" ]]; then
    curl -fsS http://127.0.0.1:5000/api/status -H "Authorization: Bearer ${token}" || true
  else
    echo "FAILED to obtain dashboard token"
  fi
else
  echo "DASHBOARD_API_KEY not found in .env"
fi

if ss -ltn 2>/dev/null | grep -qE ':(80|443)\s'; then
  echo
  echo "== Reverse proxy check =="
  curl -fsSI http://127.0.0.1/ || true
fi

echo
echo "== Outbound connectivity checks =="
targets=(
  "api.telegram.org:443"
  "api.derivws.com:443"
  "stream.binance.com:9443"
  "stream.bybit.com:443"
  "ws.okx.com:8443"
  "www.reddit.com:443"
  "bsc-dataseed1.binance.org:443"
  "api.mainnet-beta.solana.com:443"
  "s1.ripple.com:51234"
)

for target in "${targets[@]}"; do
  host="${target%%:*}"
  port="${target##*:}"
  printf "%-35s" "$target"
  if timeout 5 bash -lc "</dev/tcp/$host/$port" 2>/dev/null; then
    echo "OK"
  else
    echo "FAIL"
  fi
done

echo
echo "== Recent warning/error tail =="
tail -n 80 logs/trading_bot.log || true
