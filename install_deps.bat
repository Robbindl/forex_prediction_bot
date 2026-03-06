@echo off
    echo ========================================
    echo INSTALLING DEPENDENCIES FOR VENV311
    echo ========================================
    echo.

    cd /d C:\Users\ROBBIE\Downloads\forex_prediction_bot

    echo Activating virtual environment...
    call .\venv311\Scripts\activate.bat

    echo.
    echo Upgrading pip...
    python -m pip install --upgrade pip

    echo.
    echo Installing requirements...
    pip install -r requirements.txt

    echo.
    echo ========================================
    echo INSTALLATION COMPLETE!
    echo ========================================
    echo.
    echo To verify installation, run:
    echo    python verify_installation.py
    echo.
    pause
    