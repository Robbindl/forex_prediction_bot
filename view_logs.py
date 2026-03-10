#!/usr/bin/env python3
"""
Log viewer for trading bot
Run: python view_logs.py [--follow] [--type TYPE] [--lines N]

Options:
  --follow, -f     Follow logs in real-time (like tail -f)
  --type TYPE      Log type: all, main, errors, trades (default: all)
  --lines N        Number of lines to show (default: 50)
"""

import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

def print_header(text):
    """Print formatted header"""
    print(f"\n{'='*60}")
    print(f"📋 {text}")
    print(f"{'='*60}")

def get_log_files(log_type):
    """Get list of log files based on type"""
    log_dir = Path("logs")
    
    if not log_dir.exists():
        print(f"❌ Logs directory not found: {log_dir}")
        return []
    
    log_files = {
        "all": ["trading_bot.log", "errors.log", "trades.log"],
        "main": ["trading_bot.log"],
        "errors": ["errors.log"],
        "trades": ["trades.log"],
    }
    
    files = log_files.get(log_type, log_files["all"])
    
    # Filter only existing files
    existing = []
    for f in files:
        path = log_dir / f
        if path.exists():
            existing.append(path)
    
    return existing

def colorize_line(line):
    """Add color based on log level"""
    if "ERROR" in line or "CRITICAL" in line:
        return f"\033[91m{line}\033[0m"  # Red
    elif "WARNING" in line or "WARN" in line:
        return f"\033[93m{line}\033[0m"  # Yellow
    elif "TRADE" in line or "EXECUTED" in line:
        return f"\033[92m{line}\033[0m"  # Green
    elif "SIGNAL" in line:
        return f"\033[94m{line}\033[0m"  # Blue
    else:
        return line

def view_logs(log_type="all", lines=50):
    """View logs from different files"""
    
    files = get_log_files(log_type)
    
    if not files:
        print("❌ No log files found")
        return
    
    for log_file in files:
        print_header(f"{log_file.name} (last {lines} lines)")
        
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.readlines()
                start = max(0, len(content) - lines)
                
                for line in content[start:]:
                    print(colorize_line(line.strip()))
        except Exception as e:
            print(f"⚠️ Error reading {log_file}: {e}")

def tail_logs(log_type="all", interval=1):
    """Follow logs in real-time (like tail -f)"""
    
    files = get_log_files(log_type)
    
    if not files:
        print("❌ No log files found")
        return
    
    # Store file positions
    positions = {}
    for log_file in files:
        positions[log_file] = log_file.stat().st_size
    
    print(f"\n📡 Following logs: {', '.join([f.name for f in files])}")
    print(f"   Press Ctrl+C to stop\n")
    
    try:
        while True:
            for log_file in files:
                try:
                    # Check if file size changed
                    current_size = log_file.stat().st_size
                    
                    if current_size > positions[log_file]:
                        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                            f.seek(positions[log_file])
                            new_lines = f.readlines()
                            
                            for line in new_lines:
                                timestamp = datetime.now().strftime("%H:%M:%S")
                                prefix = f"\033[90m[{log_file.name}]\033[0m"
                                print(f"{prefix} {colorize_line(line.strip())}")
                        
                        positions[log_file] = current_size
                        
                except Exception as e:
                    print(f"⚠️ Error reading {log_file}: {e}")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n👋 Stopped following logs")

def main():
    parser = argparse.ArgumentParser(description='View trading bot logs')
    parser.add_argument('--type', '-t', 
                       choices=['all', 'main', 'errors', 'trades'], 
                       default='all',
                       help='Log type to view')
    parser.add_argument('--lines', '-n', 
                       type=int, default=50,
                       help='Number of lines to show')
    parser.add_argument('--follow', '-f', 
                       action='store_true',
                       help='Follow logs in real-time')
    
    args = parser.parse_args()
    
    if args.follow:
        tail_logs(args.type)
    else:
        view_logs(args.type, args.lines)

if __name__ == "__main__":
    main()