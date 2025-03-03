#!/usr/bin/env python3
import requests
import json
import sys
import time

class RaftMessageQueueClient:
    def __init__(self, server_url):
        """
        Initialize the client with the URL of a server node
        
        Args:
            server_url: URL of a server node (e.g., "http://127.0.0.1:5000")
        """
        self.server_url = server_url.rstrip('/')
        self.leader_url = None
        self.max_retries = 3
    
    def _handle_response(self, response):
        """Process a server response, handling leader redirection if needed"""
        if response.status_code != 200:
            return {"success": False, "error": f"HTTP error: {response.status_code}"}
        
        data = response.json()
        
        # Check if we need to redirect to leader
        if not data.get("success", True) and "leader_id" in data:
            leader_id = data["leader_id"]
            if leader_id is not None:
                self._update_leader(leader_id)
                return {"success": False, "redirect": True}
        
        return data
    
    def _update_leader(self, leader_id):
        """Update the leader URL based on a leader ID"""
        try:
            # First get a node's status to find all addresses
            response = requests.get(f"{self.server_url}/status")
            if response.status_code != 200:
                return
            
            # Find all nodes by connecting to other addresses
            # For a real implementation, we would get the cluster configuration
            # For simplicity, we'll use a hardcoded approach
            base_url = "/".join(self.server_url.split("/")[:-1])  # Remove port
            port_base = int(self.server_url.split(":")[-1])
            
            # Try to estimate the leader's URL 
            # Note: This is a simplification that works for local testing
            estimated_leader_url = f"{base_url}:{port_base + leader_id}"
            
            # Verify this is the leader
            response = requests.get(f"{estimated_leader_url}/status")
            if response.status_code == 200:
                data = response.json()
                if data.get("role") == "Leader":
                    self.leader_url = estimated_leader_url
                    print(f"Updated leader URL to {self.leader_url}")
            
        except Exception as e:
            print(f"Error updating leader: {e}")
    
    def _send_request_to_leader(self, method, endpoint, data=None, max_retries=3):
        """
        Send a request to the leader, with automatic retries and redirection
        
        Args:
            method: HTTP method ('get' or 'put')
            endpoint: API endpoint
            data: Request data (for PUT requests)
            max_retries: Maximum number of retry attempts
            
        Returns:
            Server response as a dictionary
        """
        retries = 0
        
        while retries < max_retries:
            # Use leader URL if known, otherwise use the initial server URL
            url = self.leader_url if self.leader_url else self.server_url
            
            try:
                if method.lower() == 'get':
                    response = requests.get(f"{url}{endpoint}", timeout=1)
                else:
                    response = requests.put(f"{url}{endpoint}", json=data, timeout=1)
                
                result = self._handle_response(response)
                
                # If we need to redirect to leader, retry
                if result.get("redirect", False):
                    retries += 1
                    time.sleep(0.1)  # Small delay before retry
                    continue
                
                return result
                
            except requests.RequestException as e:
                print(f"Error connecting to server: {e}")
                retries += 1
                time.sleep(0.1)  # Small delay before retry
        
        return {"success": False, "error": "Max retries exceeded"}
    
    def get_status(self):
        """Get the status of the connected node"""
        try:
            response = requests.get(f"{self.server_url}/status")
            if response.status_code == 200:
                return response.json()
            return {"success": False, "error": f"HTTP error: {response.status_code}"}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}
    
    def create_topic(self, topic):
        """
        Create a new topic on the server
        
        Args:
            topic: The name of the topic to create
            
        Returns:
            Dict with success status
        """
        return self._send_request_to_leader('put', '/topic', {"topic": topic})
    
    def get_topics(self):
        """
        Get a list of all available topics
        
        Returns:
            Dict with success status and list of topics
        """
        return self._send_request_to_leader('get', '/topic')
    
    def put_message(self, topic, message):
        """
        Add a message to a topic
        
        Args:
            topic: The topic to add the message to
            message: The message content
            
        Returns:
            Dict with success status
        """
        return self._send_request_to_leader('put', '/message', {"topic": topic, "message": message})
    
    def get_message(self, topic):
        """
        Get and remove the oldest message from a topic
        
        Args:
            topic: The topic to get a message from
            
        Returns:
            Dict with success status and message (if successful)
        """
        return self._send_request_to_leader('get', f'/message/{topic}')

def main():
    """Interactive client for testing"""
    if len(sys.argv) != 2:
        print("Usage: python client.py <server_url>")
        sys.exit(1)
    
    server_url = sys.argv[1]
    client = RaftMessageQueueClient(server_url)
    
    print(f"Connected to server at {server_url}")
    
    while True:
        print("\nAvailable commands:")
        print("1. Get server status")
        print("2. Create a topic")
        print("3. List all topics")
        print("4. Put a message")
        print("5. Get a message")
        print("6. Exit")
        
        choice = input("\nEnter command number: ")
        
        if choice == "1":
            status = client.get_status()
            print(f"Server status: {status}")
        
        elif choice == "2":
            topic = input("Enter topic name: ")
            result = client.create_topic(topic)
            print(f"Create topic result: {result}")
        
        elif choice == "3":
            topics = client.get_topics()
            print(f"Topics: {topics}")
        
        elif choice == "4":
            topic = input("Enter topic name: ")
            message = input("Enter message: ")
            result = client.put_message(topic, message)
            print(f"Put message result: {result}")
        
        elif choice == "5":
            topic = input("Enter topic name: ")
            result = client.get_message(topic)
            print(f"Get message result: {result}")
        
        elif choice == "6":
            print("Exiting...")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()