@echo off
    echo ========================================
    echo ACTIVATING VENV311
    echo ========================================
    echo.

    call "%~dp0venv311\Scriptsctivate.bat"

    echo.
    echo ========================================
    echo VIRTUAL ENVIRONMENT ACTIVATED!
    echo ========================================
    echo.
    echo Python version:
    python --version
    echo Current path: %CD%
    echo.
    echo ========================================
    echo AVAILABLE COMMANDS:
    echo ========================================
    echo.
    echo [DASHBOARD]  python web_app_live.py --balance 20
    echo [TRADING]    python trading_system.py --mode live --balance 20
    echo [STATUS]     python training_monitor.py
    echo [TRADES]     python view_trades.py
    echo.
    echo ========================================
    cmd /k
    