#!/usr/bin/env python3
"""
Quick Start Script for Forex Bot Web Application
Automatically sets up and launches the web interface
"""

import subprocess
import sys
import os
import time

def print_header():
    print("\n" + "="*70)
    print("🌐 FOREX PREDICTION BOT - WEB INTERFACE SETUP")
    print("="*70 + "\n")

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Error: Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        sys.exit(1)
    print(f"✅ Python version: {sys.version.split()[0]}")

def install_dependencies():
    """Install required packages"""
    print("\n📦 Installing web dependencies...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-q",
            "flask", "flask-cors", "pandas", "numpy", 
            "yfinance", "scikit-learn", "xgboost"
        ])
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError:
        print("⚠️  Some dependencies may already be installed")

def check_project_structure():
    """Verify project structure"""
    print("\n📁 Checking project structure...")
    
    required_dirs = ['templates', 'config', 'data', 'indicators', 'models', 'utils']
    required_files = ['web_app.py', 'templates/index.html']
    
    missing = []
    
    for directory in required_dirs:
        if not os.path.exists(directory):
            missing.append(f"Directory: {directory}")
    
    for file in required_files:
        if not os.path.exists(file):
            missing.append(f"File: {file}")
    
    if missing:
        print("❌ Missing required files/directories:")
        for item in missing:
            print(f"   - {item}")
        print("\n   Please ensure all project files are in place.")
        sys.exit(1)
    
    print("✅ Project structure verified")

def get_local_ip():
    """Get local IP address"""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "localhost"

def start_server():
    """Start the Flask web server"""
    print("\n🚀 Starting web server...\n")
    print("="*70)
    print("📊 Dashboard URLs:")
    print(f"   Local:    http://localhost:5000")
    print(f"   Network:  http://{get_local_ip()}:5000")
    print("="*70)
    print("\n💡 Tips:")
    print("   - Access from any device on your network")
    print("   - Press CTRL+C to stop the server")
    print("   - Check the dashboard for live trading signals")
    print("\n⚠️  DISCLAIMER: For educational purposes only.")
    print("   This is NOT financial advice. Trade at your own risk.")
    print("="*70 + "\n")
    
    time.sleep(2)
    
    # Start Flask app
    try:
        subprocess.run([sys.executable, "web_app.py"])
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped. Thanks for using Forex Bot!")
    except Exception as e:
        print(f"\n❌ Error starting server: {e}")
        sys.exit(1)

def main():
    """Main setup and launch sequence"""
    print_header()
    
    # Step 1: Check Python
    check_python_version()
    
    # Step 2: Install dependencies
    install_dependencies()
    
    # Step 3: Verify structure
    check_project_structure()
    
    # Step 4: Start server
    start_server()

if __name__ == "__main__":
    main()
