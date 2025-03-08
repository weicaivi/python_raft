import subprocess
import time
import requests
import json
import sys

def create_config():
    config = {
        "addresses": [
            {"ip": "http://127.0.0.1", "port": 5000}
        ]
    }
    with open("test_config.json", "w") as f:
        json.dump(config, f)
    return config

def main():
    # Create configuration
    create_config()
    
    # Start a node
    cmd = [sys.executable, "src/node.py", "test_config.json", "0"]
    process = subprocess.Popen(cmd)
    
    print("Waiting for server to start...")
    time.sleep(3)
    
    # Try to connect
    try:
        response = requests.get("http://127.0.0.1:5000/status")
        print(f"Connection successful! Response: {response.text}")
    except Exception as e:
        print(f"Connection failed: {e}")
    
    # Clean up
    process.terminate()
    process.wait()

if __name__ == "__main__":
    main()