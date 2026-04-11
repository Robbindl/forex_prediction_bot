# Azure Deployment Plan

Status: Draft

Target: Azure Linux VM with Docker Compose

Why this path:
- The bot is a long-running Python service with a dashboard, background workers, and local file state.
- A VM matches the repo's existing `docker-compose.yml` without forcing App Service storage/port changes.
- This keeps the deployment simple and avoids extra Azure services for the first cut.

Required Azure inputs:
- Ubuntu Linux VM
- One strong `POSTGRES_PASSWORD`
- Port `5000` open for the dashboard

Planned runtime layout:
- `postgres` container for the bot database
- `trading-bot` container running headless
- `web-dashboard` container exposing the UI on port `5000`

Deployment steps:
1. Create the VM and install Docker + Docker Compose.
2. Copy the repo to the VM.
3. Add `POSTGRES_PASSWORD` to the VM's `.env`.
4. Run `docker compose -f docker-compose.yml -f docker-compose.azure.yml up -d --build`.
5. Verify the dashboard at `http://<vm-public-ip>:5000`.
