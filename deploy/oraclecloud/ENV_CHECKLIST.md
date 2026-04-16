# Oracle `.env` Checklist

Use this with `deploy/oraclecloud/env.production.example` before you deploy.

## Must change before Oracle

Replace these in your real `.env`:

```env
DASHBOARD_CORS_ORIGINS=https://your-real-domain.example
DASHBOARD_API_KEY=<new-strong-random-token>
OPENAI_API_KEY=<new-openai-api-key>
DERIV_APP_ID=<your-deriv-app-id>
DERIV_TOKEN=<new-deriv-token>
COMMAND_BOT_TOKEN=<new-command-bot-token>
WHALE_TELEGRAM_TOKEN=<new-whale-bot-token>
TELEGRAM_API_ID=<your-telegram-api-id>
TELEGRAM_API_HASH=<your-telegram-api-hash>
TELEGRAM_PHONE=<your-telegram-phone>
EMAIL_PASSWORD=<new-email-app-password>
```

If your current secrets have ever been:
- committed
- screenshotted
- pasted into chats
- stored on a shared machine

rotate them before Oracle deployment.

## Must confirm

These values depend on how you deploy:

```env
DATABASE_URL=postgresql://postgres:<password>@localhost:5432/trading_bot
REDIS_URL=redis://default:<password>@<host>:6379
COMMAND_BOT_CHAT_ID=<target-chat-id>
INTELLIGENCE_CHAT_ID=<target-chat-id>
```

Rules:
- keep `localhost` in `DATABASE_URL` only if Postgres will run on the same Oracle VM
- keep the current `REDIS_URL` only if the Oracle VM can reach that Redis instance
- use the chat IDs you actually want the VPS bot to send to

## Can stay as-is

These are already reasonable defaults unless you have a specific reason to change them:

```env
DEVELOPMENT_MODE=false
TRUST_PROXY_COUNT=1
SESSION_TOKEN_TTL=3600
BINANCE_PUBLIC_DATA_ENABLED=true
ECON_CALENDAR_FOREX_FACTORY_ENABLED=true
ECON_CALENDAR_ALLOW_TRADING_ECONOMICS_GUEST=false
ECON_CALENDAR_HTTP_TIMEOUT=15
AUTO_RESEARCH_MAX_PARALLEL_ASSETS=2
ML_SERVICE_PORT=9100
LOG_LEVEL=INFO
TELEGRAM_SESSION=whale_session
BNB_RPC_URL=https://bsc-dataseed1.binance.org
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
XRPL_RPC_URL=https://s1.ripple.com:51234
```

## Recommended for a 2 vCPU / 2 GB VM

Use these as your starting performance profile on the Kamatera box:

```env
REDIS_MAX_CONNECTIONS=10
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=5
MAX_SCAN_WORKERS=4
MAX_TRAINING_WORKERS=2
AUTO_RESEARCH_MAX_PARALLEL_ASSETS=1
IG_ROUTED_CATEGORIES=commodities
IG_ROUTED_ASSETS=GER40,AUS200,JPN225
IG_ROUTE_TO_DERIV_BY_DEFAULT=false
IG_MAX_ROUTED_ASSETS=6
```

Notes:
- keep the three new regional indices on IG unless you explicitly want Deriv OTC pricing
- keep the new forex pairs on Deriv through `DERIV_SYMBOL_MAP`
- do not add `indices` to `IG_ROUTED_CATEGORIES` unless you intentionally want the full index category on IG
- use `IG_ROUTE_TO_DERIV_BY_DEFAULT=true` when you need IG primary assets to route through Deriv by default instead of exhausting IG allowance
- use `IG_MAX_ROUTED_ASSETS=<n>` to cap how many IG-routed assets are kept on IG; excess assets will fall through to Deriv proactively. The default is `6`, and setting `0` disables the cap.
- use `IG_STREAMING_HOLDOFF_SEC=<seconds>` to keep IG streaming disabled for a while after an allowance limit error, avoiding repeated retries
- raise scan or research concurrency only after checking CPU, RAM, and Redis headroom on the VPS

## Optional

These are not deployment blockers:

```env
WHALE_ALERT_KEY=
TWITTER_BEARER_TOKEN=
TWITTER_API_KEY=
TWITTER_API_SECRET=
TWITTER_ACCESS_TOKEN=
TWITTER_ACCESS_SECRET=
```

Notes:
- empty `WHALE_ALERT_KEY` means authenticated whale enrichment stays disabled
- empty Twitter keys mean Twitter-backed intelligence may be degraded or off, depending on runtime path

## Before first Oracle start

Run one of these:

Linux:

```bash
./deploy/oraclecloud/preflight.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\oraclecloud\preflight.ps1
```

The preflight should not warn about:
- `DEVELOPMENT_MODE`
- localhost CORS
- placeholder dashboard key
- placeholder database URL
- missing core runtime keys
