@echo off
cd /d %~dp0
call .\venv311\Scripts\activate
python web_app_live.py