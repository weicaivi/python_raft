#!/usr/bin/env python3
import json
import sys
import subprocess
import time
import os
import signal
import argparse

def create_config(num_nodes, base_port=5000):
    """Create a configuration file for the test cluster"""
    addresses = []
    
    for i in range(num_nodes):
        addresses.append({
            "ip": "http://127.0.0.1",
            "port": base_port + i
        })
    
    config = {"addresses": addresses}
    
    with open("config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    return config

def start_cluster(num_nodes, base_port=5000):
    """Start a test cluster with the specified number of nodes"""
    # Create configuration
    config = create_config(num_nodes, base_port)
    
    # Start nodes
    processes = []
    for i in range(num_nodes):
        cmd = [sys.executable, "src/node.py", "config.json", str(i)]
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        processes.append(process)
        print(f"Started node {i} with PID {process.pid}")
    
    return processes

def kill_node(processes, node_id):
    """Kill a specific node in the cluster"""
    if 0 <= node_id < len(processes):
        process = processes[node_id]
        if process.poll() is None:  # Process is still running
            process.terminate()
            time.sleep(0.1)
            if process.poll() is None:
                process.kill()
            print(f"Killed node {node_id}")
            return True
    return False

def restart_node(processes, node_id):
    """Restart a specific node in the cluster"""
    if kill_node(processes, node_id):
        cmd = [sys.executable, "src/node.py", "config.json", str(node_id)]
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        processes[node_id] = process
        print(f"Restarted node {node_id} with PID {process.pid}")
        return True
    return False

def stop_cluster(processes):
    """Stop all nodes in the cluster"""
    for i, process in enumerate(processes):
        if process.poll() is None:  # Process is still running
            process.terminate()
            time.sleep(0.1)
            if process.poll() is None:
                process.kill()
            print(f"Stopped node {i}")

def interactive_mode(processes):
    """Run an interactive session for testing the cluster"""
    print("\nRaft Cluster Test Interface")
    print("--------------------------")
    
    while True:
        print("\nOptions:")
        print("1. Kill a node")
        print("2. Restart a node")
        print("3. Show nodes status")
        print("4. Exit")
        
        choice = input("\nEnter your choice: ")
        
        if choice == "1":
            node_id = int(input("Enter node ID to kill: "))
            kill_node(processes, node_id)
        
        elif choice == "2":
            node_id = int(input("Enter node ID to restart: "))
            restart_node(processes, node_id)
        
        elif choice == "3":
            print("\nNode Status:")
            for i, process in enumerate(processes):
                status = "Running" if process.poll() is None else "Stopped"
                print(f"Node {i}: {status} (PID: {process.pid if process.poll() is None else 'N/A'})")
        
        elif choice == "4":
            break
        
        else:
            print("Invalid choice")

def main():
    parser = argparse.ArgumentParser(description="Raft cluster test utility")
    parser.add_argument("--nodes", type=int, default=3, help="Number of nodes in the cluster")
    parser.add_argument("--base-port", type=int, default=5000, help="Base port number")
    
    args = parser.parse_args()
    
    try:
        processes = start_cluster(args.nodes, args.base_port)
        
        # Give some time for nodes to start up
        print(f"\nStarted cluster with {args.nodes} nodes")
        print("Waiting 2 seconds for nodes to initialize...")
        time.sleep(2)
        
        interactive_mode(processes)
    
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping cluster...")
    
    finally:
        stop_cluster(processes)
        print("All nodes stopped")

if __name__ == "__main__":
    main()