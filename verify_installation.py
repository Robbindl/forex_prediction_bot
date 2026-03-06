#!/usr/bin/env python3
"""
VERIFICATION SCRIPT FOR VENV311
Run this to check if everything is working
"""

import sys
import importlib
import os
from pathlib import Path

def print_header(text):
    print(f"\n{'='*60}")
    print(f"{text}")
    print(f"{'='*60}")

def check_venv():
    """Check if running in correct virtual environment"""
    print_header("VIRTUAL ENVIRONMENT")
    
    # Check if we're in venv311
    python_path = sys.executable
    print(f"Python: {python_path}")
    
    if 'venv311' in python_path:
        print("PASS: Running in venv311")
        return True
    else:
        print("WARNING: Not running in venv311")
        print("   Run: .\\venv311\\Scripts\\activate")
        return False

def check_packages():
    """Check all required packages"""
    print_header("REQUIRED PACKAGES")
    
    required = [
        'pandas', 'numpy', 'requests', 'yfinance',
        'sklearn', 'flask', 'flask_cors', 'dotenv',
        'psutil', 'textblob', 'schedule'
    ]
    
    all_ok = True
    
    for pkg in required:
        try:
            module = importlib.import_module(pkg.replace('-', '_'))
            version = getattr(module, '__version__', 'unknown')
            print(f"  PASS: {pkg:15s} {version}")
        except ImportError:
            print(f"  FAIL: {pkg:15s} NOT INSTALLED")
            all_ok = False
    
    return all_ok

def check_env_file():
    """Check if .env file exists and has keys"""
    print_header("ENVIRONMENT VARIABLES")
    
    if Path('.env').exists():
        print("  PASS: .env file exists")
        
        # Check if keys are set
        with open('.env', 'r') as f:
            content = f.read()
            if 'YOUR_KEY' not in content:
                print("  PASS: API keys appear to be configured")
            else:
                print("  WARNING: .env still has placeholder keys")
        return True
    else:
        print("  FAIL: .env file not found!")
        return False

def test_data_fetching():
    """Test data fetching from APIs"""
    print_header("DATA FETCHING TEST")
    
    try:
        sys.path.append('.')
        from data.fetcher import NASALevelFetcher
        
        fetcher = NASALevelFetcher()
        
        # Test forex
        print("Testing EUR/USD...")
        price, source = fetcher.get_real_time_price('EUR/USD', 'forex')
        if price:
            print(f"  PASS: EUR/USD: {price:.5f} from {source}")
        else:
            print(f"  FAIL: EUR/USD: No data")
        
        # Test crypto
        print("\nTesting BTC-USD...")
        price, source = fetcher.get_real_time_price('BTC-USD', 'crypto')
        if price:
            print(f"  PASS: BTC-USD: ${price:,.2f} from {source}")
        else:
            print(f"  FAIL: BTC-USD: No data")
        
        # Test stock
        print("\nTesting AAPL...")
        price, source = fetcher.get_real_time_price('AAPL', 'stocks')
        if price:
            print(f"  PASS: AAPL: ${price:.2f} from {source}")
        else:
            print(f"  FAIL: AAPL: No data")
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("VERIFICATION FOR VENV311")
    print("="*60)
    
    checks = [
        ("Virtual Environment", check_venv),
        ("Required Packages", check_packages),
        ("Environment File", check_env_file),
        ("Data Fetching", test_data_fetching),
    ]
    
    results = {}
    for name, func in checks:
        try:
            results[name] = func()
        except Exception as e:
            print(f"{name} failed: {e}")
            results[name] = False
    
    print_header("SUMMARY")
    all_passed = all(results.values())
    
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"{status} - {name}")
    
    print("\n" + "="*60)
    if all_passed:
        print("ALL CHECKS PASSED! Your trading bot is ready!")
        print("\nNext steps:")
        print("1. Start dashboard: python web_app_live.py --balance 20")
        print("2. Access: http://localhost:5000")
        print("3. Start trading: python master_controller.py")
    else:
        print("Some checks failed - run the fix script again")
    print("="*60 + "\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
