@echo off
cd /d %~dp0
call .\venv311\Scripts\activate
python master_controller.py
pause