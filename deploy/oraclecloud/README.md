# Oracle Cloud Deployment

This bot is intended to run as a single long-lived systemd service on an Oracle Cloud VM, with Nginx reverse-proxying the dashboard.

## Recommended Small-VM Profile

For a 2 vCPU / 2 GB RAM starter VM such as your current Kamatera server, start with:

```env
REDIS_MAX_CONNECTIONS=10
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=5
MAX_SCAN_WORKERS=4
MAX_TRAINING_WORKERS=2
AUTO_RESEARCH_MAX_PARALLEL_ASSETS=1
```

That profile is intentionally conservative. It keeps the scan loop responsive without oversubscribing CPU or exhausting Redis / DB connections on a small host.

## 1. Copy the project

Suggested path:

```bash
/opt/forex_prediction_bot
```

## 2. Install runtime packages

Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip nginx nodejs npm postgresql-client redis-tools
```

## 3. Create the venv and install dependencies

```bash
cd /opt/forex_prediction_bot
python3 -m venv venv_tf
./venv_tf/bin/pip install --upgrade pip
./venv_tf/bin/pip install -r requirements.txt
./venv_tf/bin/pip install -r requirements_web.txt
```

## 4. Prepare production environment

- Start from `deploy/oraclecloud/env.production.example`
- Copy the values you actually need into `.env`
- Ensure:
  - `DEVELOPMENT_MODE=false`
  - `DASHBOARD_API_KEY` is set
  - `TRUST_PROXY_COUNT=1`
  - `DASHBOARD_CORS_ORIGINS` matches your real dashboard origin
  - `DASHBOARD_ALLOWED_HOSTS` matches the real dashboard host/domain that should be served
  - `DATABASE_URL` points to the real database
  - `REDIS_URL` points to the real Redis instance
  - `COMMAND_BOT_TOKEN` / `COMMAND_BOT_CHAT_ID` are set if you expect command alerts
  - `DEEPSEEK_TELEGRAM_TOKEN` is set if you want the standalone DeepSeek chat bot
  - `DEEPSEEK_TELEGRAM_CHAT_ID` is optional; set it if you want to lock the chat bot to one private chat
  - `WHALE_TELEGRAM_TOKEN`, `INTELLIGENCE_CHAT_ID`, and Telegram API credentials are set if you expect intelligence alerts
  - `DERIV_APP_ID` and `DERIV_TOKEN` are set for Deriv-backed data
  - `IG_ROUTED_CATEGORIES=commodities` unless you explicitly want the whole indices category on IG
  - `IG_ROUTED_ASSETS=GER40,AUS200,JPN225` if you want the new regional indices to stay on IG while the new forex pairs stay on Deriv
  - `IG_ROUTE_TO_DERIV_BY_DEFAULT=false` to preserve existing IG primary asset routing; set to `true` if you want IG primary assets to fall through to Deriv by default
  - `IG_MAX_ROUTED_ASSETS=6` to cap IG-routed assets by default and route excess assets to Deriv proactively; set to `0` to disable the cap
  - `IG_STREAMING_HOLDOFF_SEC=300` to keep IG streaming disabled for 5 minutes after an allowance limit error, avoiding repeated quota retries
  - `BINANCE_TRADFI_CONTEXT_ENABLED=true` if you want the small Binance TradFi proxy basket (`QQQ`, `SPY`, `NVDA`, `TSLA`, `EWJ`, `EWY`, `XCU`, `NATGAS`) to assist cross-asset context only
  - `BYBIT_PUBLIC_DATA_ENABLED=true` and `BYBIT_SYMBOL_MAP={"XAU/USD":"XAUUSDT","XAG/USD":"XAGUSDT","WTI":"CLUSDT"}` if you want the deeper Bybit public commodity book for gold, silver, and WTI
  - `OKX_PUBLIC_DATA_ENABLED=true` and `OKX_SYMBOL_MAP={"XAU/USD":"XAU-USDT-SWAP","XAG/USD":"XAG-USDT-SWAP","WTI":"CL-USDT-SWAP"}` as the commodity exchange-depth fallback when Bybit is not the best surface
  - rotate any secrets that have ever been committed, logged, or shared locally before deployment

## 5. Install the systemd service

```bash
sudo cp deploy/oraclecloud/forex-bot.service /etc/systemd/system/forex-bot.service
sudo systemctl daemon-reload
sudo systemctl enable forex-bot
sudo systemctl start forex-bot
sudo systemctl status forex-bot
```

If your deployment user is not `ubuntu`, update the `User`, `Group`, and `WorkingDirectory` fields in the unit first.

For a scripted install, you can also run:

```bash
chmod +x deploy/oraclecloud/install.sh
APP_DIR=/opt/forex_prediction_bot ./deploy/oraclecloud/install.sh
```

## Optional standalone DeepSeek chat bot

If you want a separate Telegram bot for pure DeepSeek chat, run it as a second process:

```bash
cd /opt/forex_prediction_bot
./venv_tf/bin/python bot.py
```

`bot.py` starts the trading stack and, if `DEEPSEEK_TELEGRAM_TOKEN` is set, the standalone DeepSeek chat bot in the background. It reads the repo `.env` file only. Set `BOT_ROLE=deepseek` only if you want this service to run DeepSeek by itself.

To run it as a service, copy `deploy/oraclecloud/deepseek-bot.service` to `/etc/systemd/system/deepseek-bot.service`, adjust the `User` and `Group` if needed, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now deepseek-bot
sudo systemctl status deepseek-bot
```

## 6. Install Nginx reverse proxy

```bash
sudo cp deploy/oraclecloud/nginx-forex-bot.conf /etc/nginx/sites-available/forex-bot
sudo ln -sf /etc/nginx/sites-available/forex-bot /etc/nginx/sites-enabled/forex-bot
sudo nginx -t
sudo systemctl reload nginx
```

Then add TLS with Certbot or your preferred certificate manager.

Important:

- keep the bot bound to `127.0.0.1:5000`
- do not expose port `5000` publicly
- let Nginx own public `80/443`

## 7. Oracle network rules

Allow inbound:

- `22/tcp` from your IP
- `80/tcp` and `443/tcp` from the internet

Do not expose publicly:

- `5000`
- `8081`
- `9100`
- `5432`

If you use `ufw`, a safe baseline is:

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw deny 5000/tcp
sudo ufw deny 8081/tcp
sudo ufw deny 9100/tcp
sudo ufw deny 5432/tcp
sudo ufw --force enable
sudo ufw status verbose
```

Allow outbound access to:

- Redis
- PostgreSQL if remote
- Telegram
- Deriv
- Binance / Bybit / OKX
- Reddit
- BNB / Solana / XRPL RPCs

## 8. Run the preflight

Linux / Oracle VM:

```bash
chmod +x deploy/oraclecloud/preflight.sh
./deploy/oraclecloud/preflight.sh
```

Windows / local PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\oraclecloud\preflight.ps1
```

This checks:

- local app ports
- production env sanity checks
- required core runtime key presence
- systemd/nginx status when available
- authenticated local `/api/status`
- outbound network reachability to key providers
- recent warning/error tail

## 9. Review the logs

Core logs:

- `logs/trading_bot.log`
- `logs/errors.log`
- `logs/trades.log`

## 10. Production notes

- The dashboard now fails closed if `DEVELOPMENT_MODE=false` and `DASHBOARD_API_KEY` is missing.
- In production mode, the dashboard prefers Hypercorn automatically instead of Flask's built-in development server.
- The economic calendar will use Deriv if supported, otherwise the ForexFactory fallback.
- The bot auto-research scheduler is enabled through `config/bot_runtime.json`; on a 2 GB VM keep `AUTO_RESEARCH_MAX_PARALLEL_ASSETS=1` unless you have measured spare CPU/RAM headroom.
- The trading engine now reads `MAX_SCAN_WORKERS` from env, so tune concurrency in `.env` instead of editing code before moving between machines.
- Commodity exchange depth is now layered: Bybit first for `XAU/USD`, `XAG/USD`, and `WTI`, with OKX retained as fallback where Bybit's public API is not the cleanest fit.
- A small Binance TradFi proxy basket can now feed cross-asset context (`QQQ`, `SPY`, `NVDA`, `TSLA`, `EWJ`, `EWY`, `XCU`, `NATGAS`) without making those symbols part of the primary tradeable universe.
- Replace `server_name _;` in `deploy/oraclecloud/nginx-forex-bot.conf` with your real domain before turning on TLS.
- The bundled preflight warns on placeholder `DASHBOARD_API_KEY`, localhost CORS, and template `DATABASE_URL` values before you go live.
