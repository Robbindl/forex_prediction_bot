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
  - `DATABASE_URL` points to the real database
  - `REDIS_URL` points to the real Redis instance
  - `COMMAND_BOT_TOKEN` / `COMMAND_BOT_CHAT_ID` are set if you expect command alerts
  - `WHALE_TELEGRAM_TOKEN`, `INTELLIGENCE_CHAT_ID`, and Telegram API credentials are set if you expect intelligence alerts
  - `DERIV_APP_ID` and `DERIV_TOKEN` are set for Deriv-backed data
  - `IG_ROUTED_ASSETS=GER40,AUS200,JPN225` if you want the new regional indices to stay on IG while the new forex pairs stay on Deriv
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

## 6. Install Nginx reverse proxy

```bash
sudo cp deploy/oraclecloud/nginx-forex-bot.conf /etc/nginx/sites-available/forex-bot
sudo ln -sf /etc/nginx/sites-available/forex-bot /etc/nginx/sites-enabled/forex-bot
sudo nginx -t
sudo systemctl reload nginx
```

Then add TLS with Certbot or your preferred certificate manager.

## 7. Oracle network rules

Allow inbound:

- `22/tcp` from your IP
- `80/tcp` and `443/tcp` from the internet

Do not expose publicly:

- `5000`
- `8081`
- `9100`
- `5432`

Allow outbound access to:

- Redis
- PostgreSQL if remote
- Telegram
- Deriv
- Binance / Bybit
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
- Replace `server_name _;` in `deploy/oraclecloud/nginx-forex-bot.conf` with your real domain before turning on TLS.
- The bundled preflight warns on placeholder `DASHBOARD_API_KEY`, localhost CORS, and template `DATABASE_URL` values before you go live.
