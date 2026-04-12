# Azure Deployment Plan

Status: Draft

Target: Azure Linux VM with Docker Compose

Why this path:
- The bot is a long-running Python service with a dashboard, background workers, and local file state.
- A VM matches the repo's Docker workflow and lets the bot serve the dashboard itself.
- For durability, the bot should use Azure Database for PostgreSQL Flexible Server instead of a local VM disk.
- For file durability, use Azure Files for the bot's runtime folders.

Required Azure inputs:
- Ubuntu Linux VM
- Azure Database for PostgreSQL Flexible Server
- Azure Storage account
- Three Azure File Shares: `data`, `logs`, `state`
- One strong database password
- One storage account key
- Port `5000` open for the dashboard

Planned runtime layout:
- One `trading-bot` container that also exposes the UI on port `5000`
- Azure-managed PostgreSQL as the durable data store
- Azure Files-backed `data/`, `logs/`, and `state/` folders
- `data/` holds bot state and local candle cache
- `logs/` holds application and reset logs
- `state/` holds Telegram session and PID files

Deployment steps:
1. Create the VM and install Docker, Docker Compose, and `cifs-utils`.
2. Create Azure Database for PostgreSQL Flexible Server and allow the VM to connect.
3. Create the Azure File Shares for `data`, `logs`, and `state`.
4. Copy the repo to the VM.
5. Update the VM's `.env` with the Azure PostgreSQL connection string and storage account details.
6. Run `docker compose -f docker-compose.azure-manageddb.yml up -d --build`.
7. Verify the dashboard at `http://<vm-public-ip>:5000`.
