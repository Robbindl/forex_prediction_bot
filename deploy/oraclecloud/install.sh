#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/forex_prediction_bot}"
SERVICE_NAME="${SERVICE_NAME:-forex-bot}"
DASHBOARD_SERVICE_NAME="${DASHBOARD_SERVICE_NAME:-forex-dashboard}"
DASHBOARD_WATCHDOG_SERVICE_NAME="${DASHBOARD_WATCHDOG_SERVICE_NAME:-forex-dashboard-watchdog}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-forex-bot}"

if [[ ! -d "${APP_DIR}" ]]; then
  echo "FAIL: APP_DIR does not exist: ${APP_DIR}"
  exit 1
fi

cd "${APP_DIR}"

if [[ ! -f ".env" ]]; then
  echo "FAIL: ${APP_DIR}/.env is missing"
  echo "Copy deploy/oraclecloud/env.production.example to .env and fill it first."
  exit 1
fi

echo "Installing systemd unit..."
sudo cp deploy/oraclecloud/forex-bot.service "/etc/systemd/system/${SERVICE_NAME}.service"
sudo sed -i "s|/opt/forex_prediction_bot|${APP_DIR}|g" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo sed -i "s|User=ubuntu|User=${SUDO_USER:-$(whoami)}|g" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo sed -i "s|Group=ubuntu|Group=${SUDO_USER:-$(whoami)}|g" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo cp deploy/oraclecloud/forex-dashboard.service "/etc/systemd/system/${DASHBOARD_SERVICE_NAME}.service"
sudo sed -i "s|/opt/forex_prediction_bot|${APP_DIR}|g" "/etc/systemd/system/${DASHBOARD_SERVICE_NAME}.service"
sudo sed -i "s|User=ubuntu|User=${SUDO_USER:-$(whoami)}|g" "/etc/systemd/system/${DASHBOARD_SERVICE_NAME}.service"
sudo sed -i "s|Group=ubuntu|Group=${SUDO_USER:-$(whoami)}|g" "/etc/systemd/system/${DASHBOARD_SERVICE_NAME}.service"
sudo cp deploy/oraclecloud/forex-dashboard-watchdog.service "/etc/systemd/system/${DASHBOARD_WATCHDOG_SERVICE_NAME}.service"
sudo sed -i "s|/opt/forex_prediction_bot|${APP_DIR}|g" "/etc/systemd/system/${DASHBOARD_WATCHDOG_SERVICE_NAME}.service"
sudo sed -i "s|forex-dashboard|${DASHBOARD_SERVICE_NAME}|g" "/etc/systemd/system/${DASHBOARD_WATCHDOG_SERVICE_NAME}.service"
sudo cp deploy/oraclecloud/forex-dashboard-watchdog.timer "/etc/systemd/system/${DASHBOARD_WATCHDOG_SERVICE_NAME}.timer"
sudo sed -i "s|forex-dashboard-watchdog|${DASHBOARD_WATCHDOG_SERVICE_NAME}|g" "/etc/systemd/system/${DASHBOARD_WATCHDOG_SERVICE_NAME}.timer"
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"
sudo systemctl enable "${DASHBOARD_SERVICE_NAME}"
sudo systemctl enable "${DASHBOARD_WATCHDOG_SERVICE_NAME}.timer"
sudo systemctl restart "${SERVICE_NAME}"
sudo systemctl restart "${DASHBOARD_SERVICE_NAME}"
sudo systemctl restart "${DASHBOARD_WATCHDOG_SERVICE_NAME}.timer"

echo "Installing Nginx site..."
sudo cp deploy/oraclecloud/nginx-forex-bot.conf "/etc/nginx/sites-available/${NGINX_SITE_NAME}"
sudo ln -sf "/etc/nginx/sites-available/${NGINX_SITE_NAME}" "/etc/nginx/sites-enabled/${NGINX_SITE_NAME}"
if [[ -L /etc/nginx/sites-enabled/default ]]; then
  sudo rm -f /etc/nginx/sites-enabled/default
fi
sudo nginx -t
sudo systemctl reload nginx

echo
echo "Running local preflight..."
chmod +x deploy/oraclecloud/preflight.sh
./deploy/oraclecloud/preflight.sh

echo
echo "Install complete."
echo "Review service status with: sudo systemctl status ${SERVICE_NAME}"
echo "Review dashboard status with: sudo systemctl status ${DASHBOARD_SERVICE_NAME}"
echo "Review dashboard watchdog with: sudo systemctl status ${DASHBOARD_WATCHDOG_SERVICE_NAME}.timer"
