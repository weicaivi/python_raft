import subprocess
import sys
import time

def main():
    config_file = sys.argv[1] if len(sys.argv) > 1 else "test_config.json"
    num_nodes = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    
    processes = []
    
    try:
        for i in range(num_nodes):
            cmd = [sys.executable, "src/node.py", config_file, str(i)]
            process = subprocess.Popen(cmd)
            processes.append(process)
            print(f"Started node {i}, PID: {process.pid}")
            time.sleep(0.5)  # Small delay between node starts
        
        print("All nodes started. Press Ctrl+C to terminate")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Terminating nodes...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    main()