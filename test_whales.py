from whale_alert_manager import WhaleAlertManager
import time

# Initialize
manager = WhaleAlertManager()
manager.start_monitoring()

print("Waiting for whale alerts...")
print("Press Ctrl+C to stop")

try:
    while True:
        time.sleep(10)
        alerts = manager.get_alerts()
        if alerts:
            print(f"\n🐋 Found {len(alerts)} whale alerts:")
            for alert in alerts[:3]:
                print(f"  {alert['title']} - {alert['source']}")
        else:
            print(".", end="", flush=True)
except KeyboardInterrupt:
    manager.stop()
    print("\nStopped")