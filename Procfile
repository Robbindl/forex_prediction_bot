web: gunicorn web_app:app
web: python web_app_live.py --balance 30
worker: python trading_system.py --mode live --balance 30 --strategy-mode voting