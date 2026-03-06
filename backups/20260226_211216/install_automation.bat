@echo off
echo 🚀 Installing Complete Trading Bot Automation...
echo.

:: Create all batch files
echo Creating batch files...

echo @echo off > start_master.bat
echo cd /d %~dp0 >> start_master.bat
echo call .\venv311\Scripts\activate >> start_master.bat
echo python master_controller.py >> start_master.bat

echo @echo off > start_dashboard.bat
echo cd /d %~dp0 >> start_dashboard.bat
echo call .\venv311\Scripts\activate >> start_dashboard.bat
echo python web_app_live.py >> start_dashboard.bat

:: Install Python packages
echo Installing required packages...
call .\venv311\Scripts\activate
pip install psutil schedule

:: Create folders
mkdir logs 2>nul
mkdir reports 2>nul
mkdir backups 2>nul

echo.
echo ✅ Setup Complete!
echo.
echo Your trading bot will now run 24/7 automatically!
echo Access dashboard at: http://localhost:5000
echo Check logs at: C:\Users\ROBBIE\Downloads\forex_prediction_bot\master_controller.log
echo.
pause