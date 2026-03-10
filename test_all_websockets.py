"""
TEST 4: Complete WebSocket Test Suite
Runs all tests sequentially - FIXED VERSION
"""

import subprocess
import sys
import time

# List of test files to run (NOT including this file!)
TEST_FILES = [
    "test_websocket_basic.py",
    "test_websocket_manager.py", 
    "test_websocket_integration.py"
]

def run_test(test_file):
    """Run a single test file"""
    print("\n" + "="*70)
    print(f"🚀 RUNNING: {test_file}")
    print("="*70)
    
    try:
        # Run the test file
        result = subprocess.run([sys.executable, test_file], 
                               capture_output=True, 
                               text=True)
        
        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("❌ ERRORS:")
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"\n✅ {test_file} PASSED")
            return True
        else:
            print(f"\n❌ {test_file} FAILED (exit code: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"\n❌ Error running {test_file}: {e}")
        return False

def main():
    print("="*70)
    print("📡 COMPLETE WEBSOCKET TEST SUITE")
    print("="*70)
    print(f"\n📋 Tests to run: {len(TEST_FILES)}")
    for f in TEST_FILES:
        print(f"   • {f}")
    
    results = []
    
    for test_file in TEST_FILES:
        print(f"\n⏱️ Starting {test_file} in 2 seconds...")
        time.sleep(2)
        
        success = run_test(test_file)
        results.append((test_file, success))
        
        # Ask to continue if failed
        if not success:
            print(f"\n⚠️ {test_file} failed. Continue with next test? (y/n): ", end="")
            response = input().lower().strip()
            if response != 'y':
                break
    
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 ALL TESTS PASSED! WebSocket system is working!")
    else:
        print("\n⚠️ Some tests failed. Check the errors above.")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())