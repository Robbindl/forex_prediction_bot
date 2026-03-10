"""
HEALTH CHECK - Ensures everything is running
Runs every hour via Task Scheduler
"""

import subprocess
import psutil
import os
import sys
import json
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
import requests
from pathlib import Path

# Add project to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from logger import logger

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

def send_email_alert(message):
    """Send email alert"""
    try:
        sender = "griffonstradingbot@gmail.com"
        password = "erfo hjsp hmwj pgqc"
        receiver = "griffonstradingbot@gmail.com"
        
        msg = MIMEText(message)
        msg['Subject'] = f"🚨 Trading Bot Alert - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg['From'] = sender
        msg['To'] = receiver
        
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        logger.info("Email alert sent")
        return True
    except Exception as e:
        logger.error(f"Could not send email: {e}")
        return False

def send_telegram_alert(message):
    """Send Telegram alert using existing bot"""
    try:
        # Load Telegram config
        config_path = Path("config/telegram_config.json")
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            bot_token = config.get('bot_token')
            chat_id = config.get('chat_id')
            
            if bot_token and chat_id:
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                data = {
                    'chat_id': chat_id,
                    'text': f"🔍 *Health Check Alert*\n\n{message}",
                    'parse_mode': 'Markdown'
                }
                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 200:
                    logger.info("Telegram alert sent")
                    return True
    except Exception as e:
        logger.error(f"Could not send Telegram: {e}")
    return False

def check_disk_space():
    """Check if disk space is running low"""
    try:
        disk = psutil.disk_usage('/')
        free_gb = disk.free / (1024**3)
        total_gb = disk.total / (1024**3)
        percent_free = (disk.free / disk.total) * 100
        
        if percent_free < 10:
            return False, f"Low disk space: {free_gb:.1f}GB free ({percent_free:.1f}%)"
        return True, f"Disk OK: {free_gb:.1f}GB free"
    except:
        return True, "Disk check unavailable"

def check_memory():
    """Check if memory is running low"""
    try:
        memory = psutil.virtual_memory()
        if memory.percent > 90:
            return False, f"High memory usage: {memory.percent}%"
        return True, f"Memory: {memory.percent}% used"
    except:
        return True, "Memory check unavailable"

def check_cpu():
    """Check if CPU is overloaded"""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 95:
            return False, f"High CPU usage: {cpu_percent}%"
        return True, f"CPU: {cpu_percent}%"
    except:
        return True, "CPU check unavailable"

def restart_component(component):
    """Restart a specific component"""
    try:
        if component == "master_controller":
            subprocess.Popen(["python", "master_controller.py"])
            return True
        elif component == "trading_system":
            subprocess.Popen(["python", "trading_system.py", "--mode", "live", "--balance", "30", "--no-telegram"])
            return True
        elif component == "web_app_live":
            subprocess.Popen(["python", "web_app_live.py", "--balance", "30"])
            return True
        elif component == "performance_dashboard":
            subprocess.Popen(["python", "performance_dashboard.py"])
            return True
    except Exception as e:
        logger.error(f"Failed to restart {component}: {e}")
    return False

def main():
    print(f"\n{'='*60}")
    print(f"🔍 HEALTH CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    # Track issues
    issues = []
    warnings = []
    
    # Check all components
    components = {
        "master_controller": "master_controller.py",
        "trading_system": "trading_system.py",
        "web_app_live": "web_app_live.py",
        "performance_dashboard": "performance_dashboard.py",
        "realtime_trader": "realtime_trader.py",
        "telegram_bot": "telegram_commander.py"
    }
    
    print("\n📋 COMPONENT STATUS:")
    print("-" * 50)
    
    running_count = 0
    for name, script in components.items():
        if check_process(script):
            print(f"  ✅ {name:20} RUNNING")
            running_count += 1
        else:
            print(f"  ❌ {name:20} NOT RUNNING")
            issues.append(f"{name} is down")
    
    # System health checks
    print("\n🖥️ SYSTEM HEALTH:")
    print("-" * 50)
    
    disk_ok, disk_msg = check_disk_space()
    print(f"  {'✅' if disk_ok else '⚠️'} Disk: {disk_msg}")
    if not disk_ok:
        warnings.append(disk_msg)
    
    mem_ok, mem_msg = check_memory()
    print(f"  {'✅' if mem_ok else '⚠️'} Memory: {mem_msg}")
    if not mem_ok:
        warnings.append(mem_msg)
    
    cpu_ok, cpu_msg = check_cpu()
    print(f"  {'✅' if cpu_ok else '⚠️'} CPU: {cpu_msg}")
    if not cpu_ok:
        warnings.append(cpu_msg)
    
    # Check if any models are too old
    try:
        from training_monitor import TrainingMonitor
        monitor = TrainingMonitor()
        ages = monitor.get_model_ages()
        old_models = [name for name, data in ages.items() if data.get('age_days', 0) > 7]
        if old_models:
            print(f"  ⚠️ Models: {len(old_models)} models need retraining")
            warnings.append(f"{len(old_models)} models >7 days old")
        else:
            print(f"  ✅ Models: All {len(ages)} models fresh")
    except:
        pass
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 SUMMARY:")
    print(f"  • Components: {running_count}/{len(components)} running")
    print(f"  • Issues: {len(issues)}")
    print(f"  • Warnings: {len(warnings)}")
    print(f"{'='*60}")
    
    # Take action if needed
    if issues:
        print("\n🚨 ISSUES DETECTED:")
        for issue in issues:
            print(f"  • {issue}")
        
        # Try to restart master if multiple components down
        if len(issues) >= 2 or "master_controller" in str(issues):
            print("\n🔄 Restarting master controller...")
            if restart_component("master_controller"):
                message = f"⚠️ Master controller restarted at {datetime.now().strftime('%H:%M:%S')}\nIssues: {', '.join(issues)}"
                send_email_alert(message)
                send_telegram_alert(message)
        else:
            # Try to restart individual components
            for name, script in components.items():
                if not check_process(script) and name != "master_controller":
                    print(f"🔄 Restarting {name}...")
                    if restart_component(name):
                        message = f"✅ {name} restarted at {datetime.now().strftime('%H:%M:%S')}"
                        send_telegram_alert(message)
    
    elif warnings:
        print("\n⚠️ WARNINGS:")
        for warning in warnings:
            print(f"  • {warning}")
        
        # Send warning notification (but don't restart)
        if warnings:
            message = f"⚠️ Health warnings at {datetime.now().strftime('%H:%M:%S')}\n{', '.join(warnings)}"
            send_telegram_alert(message)
    else:
        print("\n✅ ALL SYSTEMS HEALTHY")
        
        # Send occasional all-clear (every 6 hours)
        if datetime.now().hour % 6 == 0 and datetime.now().minute < 10:
            send_telegram_alert(f"✅ All systems healthy - {running_count}/{len(components)} components running")
    
    # Log health status
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'components_running': running_count,
        'total_components': len(components),
        'issues': issues,
        'warnings': warnings,
        'status': 'OK' if not issues else 'ISSUES'
    }
    
    with open('health_log.json', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    
    # Simple text log for backward compatibility
    with open('health_log.txt', 'a') as f:
        status = 'OK' if not issues else 'ISSUES'
        f.write(f"{datetime.now()} - {status} - {running_count}/{len(components)} running\n")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    main()