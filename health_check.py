"""
HEALTH CHECK - Ensures everything is running
Runs every hour via Task Scheduler
"""

import subprocess
import psutil
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def check_process(name):
    """Check if process is running"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmd_line = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
            if name in cmd_line:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False

def send_alert(message):
    """Send email alert"""
    try:
        # Configure your email here
        sender = "griffonstradingbot@gmail.com"
        password = "erfo hjsp hmwj pgqc"
        receiver = "griffonstradingbot@gmail.com"
        
        msg = MIMEText(message)
        msg['Subject'] = "Trading Bot Alert"
        msg['From'] = sender
        msg['To'] = receiver
        
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        print(" Alert email sent")
    except Exception as e:
        print(f" Could not send email: {e}")

def main():
    print(f"\nHealth Check at {datetime.now()}")
    print("-"*50)
    
    # Check all components
    components = {
        "master_controller": "master_controller.py",
        "trading_system": "trading_system.py",
        "web_app_live": "web_app_live.py",
        "performance_dashboard": "performance_dashboard.py"
    }
    
    all_good = True
    
    for name, script in components.items():
        if check_process(script):
            print(f" [OK] {name}: RUNNING")
        else:
            print(f" [FAIL] {name}: NOT RUNNING")
            all_good = False
    
    # If something died, restart master (which restarts everything)
    if not all_good:
        print(" [WARNING] Some components down - restarting master...")
        try:
            subprocess.Popen(["python", "master_controller.py"])
            send_alert("Trading bot restarted - some components were down")
        except Exception as e:
            print(f" Restart failed: {e}")
    
    # Log health status
    with open('health_log.txt', 'a') as f:
        f.write(f"{datetime.now()} - {'OK' if all_good else 'RESTARTED'}\n")
    
    print("\n" + "-"*50)
    if all_good:
        print("✓ ALL SYSTEMS HEALTHY")
    else:
        print("! SOME ISSUES DETECTED - Check logs")
    print("-"*50)

if __name__ == "__main__":
    main()