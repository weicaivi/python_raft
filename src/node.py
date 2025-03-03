#!/usr/bin/env python3
import json
import sys
import time
import random
import threading
import requests
from collections import defaultdict, deque
from flask import Flask, request, jsonify

# Constants for node roles
FOLLOWER = "Follower"
CANDIDATE = "Candidate"
LEADER = "Leader"

# Constants for timeouts (in seconds)
ELECTION_TIMEOUT_MIN = 0.15
ELECTION_TIMEOUT_MAX = 0.3
HEARTBEAT_INTERVAL = 0.05

# Constants for log entry types (operations)
OP_CREATE_TOPIC = "CREATE_TOPIC"
OP_PUT_MESSAGE = "PUT_MESSAGE"
OP_GET_MESSAGE = "GET_MESSAGE"

class LogEntry:
    """Represents a single entry in the Raft log"""
    def __init__(self, term, operation, data):
        self.term = term
        self.operation = operation
        self.data = data
        
    def to_dict(self):
        return {
            "term": self.term,
            "operation": self.operation,
            "data": self.data
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(data["term"], data["operation"], data["data"])

class RaftNode:
    def __init__(self, config_path, node_id):
        # Load configuration
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        self.node_id = int(node_id)
        self.addresses = config["addresses"]
        self.address = self.addresses[self.node_id]
        
        # Initialize Raft state
        self.current_term = 0
        self.voted_for = None
        self.role = FOLLOWER
        self.leader_id = None
        self.votes_received = 0
        
        # Initialize log
        self.log = []  # List of LogEntry objects
        self.commit_index = -1  # Index of highest log entry known to be committed
        self.last_applied = -1  # Index of highest log entry applied to state machine
        
        # Leader-specific state
        self.next_index = {}    # For each server, index of next log entry to send
        self.match_index = {}   # For each server, index of highest log entry known to be replicated
        
        # Initialize state machine (message queue)
        self.topics = []
        self.messages = defaultdict(deque)  # topic -> queue of messages
        
        # Lock for state modifications
        self.state_lock = threading.RLock()
        self.apply_lock = threading.RLock()  # Separate lock for applying to state machine
        
        # Initialize election timer
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        
        print(f"Node {self.node_id} starting as {self.role} with {len(self.addresses)} nodes in the cluster")
        
        # Start background threads
        self.timer_thread = threading.Thread(target=self._election_timer, daemon=True)
        self.timer_thread.start()
        
        self.apply_thread = threading.Thread(target=self._apply_committed_entries, daemon=True)
        self.apply_thread.start()
        
        # For single node setup, immediately become leader
        if len(self.addresses) == 1:
            self._become_leader()
    
    def _random_election_timeout(self):
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    
    def _election_timer(self):
        """Monitor for election timeouts and start elections when needed"""
        while True:
            time.sleep(0.01)  # Small sleep to prevent CPU hogging
            
            with self.state_lock:
                # Skip timer logic if we're the leader
                if self.role == LEADER:
                    continue
                
                # Check if election timeout has elapsed
                if time.time() - self.last_heartbeat > self.election_timeout:
                    print(f"Election timeout - starting election for node {self.node_id}")
                    self._start_election()
    
    def _apply_committed_entries(self):
        """Background thread to apply committed log entries to the state machine"""
        while True:
            time.sleep(0.01)  # Small sleep to prevent CPU hogging
            
            with self.state_lock:
                # Skip if nothing to apply
                if self.last_applied >= self.commit_index:
                    continue
                
                last_applied = self.last_applied
                commit_index = self.commit_index
            
            # Apply all committed but not yet applied entries
            for i in range(last_applied + 1, commit_index + 1):
                self._apply_log_entry(i)
                
                with self.state_lock:
                    self.last_applied = i
    
    def _apply_log_entry(self, index):
        """Apply a log entry to the state machine"""
        with self.apply_lock:
            if index < 0 or index >= len(self.log):
                return False
            
            entry = self.log[index]
            operation = entry.operation
            data = entry.data
            
            print(f"Node {self.node_id} applying operation {operation} at index {index}")
            
            if operation == OP_CREATE_TOPIC:
                topic = data["topic"]
                if topic not in self.topics:
                    self.topics.append(topic)
                    print(f"Node {self.node_id} created topic: {topic}")
            
            elif operation == OP_PUT_MESSAGE:
                topic = data["topic"]
                message = data["message"]
                if topic in self.topics:
                    self.messages[topic].append(message)
                    print(f"Node {self.node_id} added message to topic {topic}: {message}")
            
            elif operation == OP_GET_MESSAGE:
                topic = data["topic"]
                if topic in self.topics and self.messages[topic]:
                    message = self.messages[topic].popleft()
                    print(f"Node {self.node_id} got message from topic {topic}: {message}")
            
            return True
    
    def _start_election(self):
        """Start a new election"""
        with self.state_lock:
            self.role = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1  # Vote for self
            self.election_timeout = self._random_election_timeout()
            self.last_heartbeat = time.time()
            
            print(f"Node {self.node_id} starting election for term {self.current_term}")
            
            # In single-node mode, immediately become leader
            if len(self.addresses) == 1:
                self._become_leader()
                return
        
        # Request votes from all other nodes
        self._request_votes()
    
    def _request_votes(self):
        """Send RequestVote RPCs to all other nodes"""
        for i, address in enumerate(self.addresses):
            if i == self.node_id:
                continue
            
            # Create a thread for each RequestVote RPC
            threading.Thread(
                target=self._send_request_vote,
                args=(i, address),
                daemon=True
            ).start()
    
    def _send_request_vote(self, target_id, address):
        """Send RequestVote RPC to a specific node"""
        try:
            with self.state_lock:
                if self.role != CANDIDATE:
                    return
                
                term = self.current_term
                
                # Get lastLogIndex and lastLogTerm
                last_log_index = len(self.log) - 1
                last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
            
            url = f"{address['ip']}:{address['port']}/request_vote"
            data = {
                "term": term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            
            response = requests.post(url, json=data, timeout=0.1)
            response_data = response.json()
            
            with self.state_lock:
                # If we received a higher term, update and become follower
                if response_data["term"] > self.current_term:
                    self.current_term = response_data["term"]
                    self._become_follower(None)
                    return
                
                # If we're no longer a candidate or term changed, ignore the response
                if self.role != CANDIDATE or term != self.current_term:
                    return
                
                # If the vote was granted, count it
                if response_data["vote_granted"]:
                    self.votes_received += 1
                    print(f"Node {self.node_id} received vote from {target_id}, total: {self.votes_received}")
                    
                    # Check if we have majority votes
                    if self.votes_received > len(self.addresses) // 2:
                        self._become_leader()
        
        except Exception as e:
            print(f"Error requesting vote from node {target_id}: {e}")
    
    def _become_leader(self):
        """Transition to leader state"""
        with self.state_lock:
            if self.role != CANDIDATE:
                return
            
            self.role = LEADER
            self.leader_id = self.node_id
            print(f"Node {self.node_id} became leader for term {self.current_term}")
            
            # Initialize leader state
            last_log_index = len(self.log) - 1
            for i in range(len(self.addresses)):
                if i != self.node_id:
                    self.next_index[i] = last_log_index + 1
                    self.match_index[i] = -1
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
    
    def _become_follower(self, leader_id):
        """Transition to follower state"""
        with self.state_lock:
            old_role = self.role
            self.role = FOLLOWER
            self.leader_id = leader_id
            self.voted_for = None
            self.reset_election_timeout()
            
            if old_role != FOLLOWER:
                print(f"Node {self.node_id} became follower, leader is node {leader_id}")
    
    def _send_heartbeats(self):
        """Send heartbeats (AppendEntries) to all followers periodically"""
        while True:
            with self.state_lock:
                if self.role != LEADER:
                    return
                
                self._replicate_logs_to_all()
            
            time.sleep(HEARTBEAT_INTERVAL)
    
    def _replicate_logs_to_all(self):
        """Replicate logs to all followers"""
        for i, address in enumerate(self.addresses):
            if i == self.node_id:
                continue
            
            # Create a thread for each follower
            threading.Thread(
                target=self._replicate_logs_to_follower,
                args=(i, address),
                daemon=True
            ).start()
    
    def _replicate_logs_to_follower(self, follower_id, address):
        """Replicate logs to a specific follower"""
        try:
            with self.state_lock:
                if self.role != LEADER:
                    return
                
                # Get nextIndex for this follower
                next_index = self.next_index.get(follower_id, 0)
                
                # Get prevLogIndex and prevLogTerm
                prev_log_index = next_index - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                
                # Get entries to send
                entries = self.log[next_index:] if next_index < len(self.log) else []
                entries_dict = [entry.to_dict() for entry in entries]
                
                # Get commitIndex
                leader_commit = self.commit_index
                term = self.current_term
            
            # Send AppendEntries RPC
            url = f"{address['ip']}:{address['port']}/append_entries"
            data = {
                "term": term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries_dict,
                "leader_commit": leader_commit
            }
            
            response = requests.post(url, json=data, timeout=0.1)
            response_data = response.json()
            
            with self.state_lock:
                # If we received a higher term, update and become follower
                if response_data["term"] > self.current_term:
                    self.current_term = response_data["term"]
                    self._become_follower(None)
                    return
                
                # If we're no longer the leader, stop replication
                if self.role != LEADER or term != self.current_term:
                    return
                
                # If AppendEntries was successful
                if response_data["success"]:
                    if entries:
                        # Update matchIndex and nextIndex for this follower
                        match_index = prev_log_index + len(entries)
                        self.match_index[follower_id] = match_index
                        self.next_index[follower_id] = match_index + 1
                        
                        print(f"Node {self.node_id} successfully replicated logs to node {follower_id}, matchIndex: {match_index}")
                        
                        # Check if we can update commitIndex
                        self._update_commit_index()
                else:
                    # If AppendEntries failed because of log inconsistency, decrement nextIndex and retry
                    if "conflict_index" in response_data:
                        # Use the conflict index provided by the follower
                        self.next_index[follower_id] = response_data["conflict_index"]
                    else:
                        # Decrement nextIndex and try again
                        self.next_index[follower_id] = max(0, self.next_index[follower_id] - 1)
                    
                    print(f"Node {self.node_id} failed to replicate logs to node {follower_id}, new nextIndex: {self.next_index[follower_id]}")
        
        except Exception as e:
            print(f"Error replicating logs to node {follower_id}: {e}")
    
    def _update_commit_index(self):
        """Update commit_index if there exists an N such that:
        - N > commit_index
        - A majority of matchIndex[i] ≥ N
        - log[N].term == currentTerm
        """
        with self.state_lock:
            if self.role != LEADER:
                return
            
            # Get all match indices (including leader's own log)
            match_indices = list(self.match_index.values()) + [len(self.log) - 1]
            
            # Sort match indices in descending order
            match_indices.sort(reverse=True)
            
            # Get the median index (majority replicated)
            majority_index = len(self.addresses) // 2
            n = match_indices[majority_index]
            
            # Check if N > commit_index and log[N].term == currentTerm
            if n > self.commit_index and (n < len(self.log) and self.log[n].term == self.current_term):
                old_commit_index = self.commit_index
                self.commit_index = n
                print(f"Node {self.node_id} updated commit_index from {old_commit_index} to {n}")
    
    def reset_election_timeout(self):
        """Reset the election timeout"""
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
    
    def handle_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        """Handle a RequestVote RPC"""
        with self.state_lock:
            # If the candidate's term is lower than ours, reject the vote
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}
            
            # If we see a higher term, update our term and become a follower
            if term > self.current_term:
                self.current_term = term
                self._become_follower(None)
            
            # Check if the candidate's log is at least as up-to-date as ours
            my_last_log_index = len(self.log) - 1
            my_last_log_term = self.log[my_last_log_index].term if my_last_log_index >= 0 else 0
            
            log_is_up_to_date = (
                last_log_term > my_last_log_term or
                (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
            )
            
            # Determine whether to grant the vote
            vote_granted = (
                (self.voted_for is None or self.voted_for == candidate_id) and
                log_is_up_to_date
            )
            
            if vote_granted:
                self.voted_for = candidate_id
                print(f"Node {self.node_id} voted for {candidate_id} in term {term}")
                self.reset_election_timeout()
            
            return {"term": self.current_term, "vote_granted": vote_granted}
    
    def handle_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        """Handle an AppendEntries RPC"""
        with self.state_lock:
            # If the leader's term is lower than ours, reject
            if term < self.current_term:
                return {"term": self.current_term, "success": False}
            
            # If we see a higher term, update our term
            if term > self.current_term:
                self.current_term = term
            
            # Recognize the leader
            self._become_follower(leader_id)
            self.reset_election_timeout()
            
            # Check if our log matches the leader's at prevLogIndex
            if prev_log_index >= 0:
                if prev_log_index >= len(self.log):
                    # Our log is too short
                    return {
                        "term": self.current_term, 
                        "success": False,
                        "conflict_index": len(self.log)
                    }
                
                if self.log[prev_log_index].term != prev_log_term:
                    # Term mismatch at prevLogIndex
                    # Find the first index of this term
                    conflict_term = self.log[prev_log_index].term
                    conflict_index = prev_log_index
                    
                    # Find the first index with this term
                    while conflict_index > 0 and self.log[conflict_index - 1].term == conflict_term:
                        conflict_index -= 1
                    
                    return {
                        "term": self.current_term, 
                        "success": False,
                        "conflict_index": conflict_index
                    }
            
            # If we get here, the log matches at prevLogIndex
            
            # Process the entries
            if entries:
                # Convert dict entries to LogEntry objects
                new_entries = [LogEntry.from_dict(entry) for entry in entries]
                
                # If we have existing entries that conflict with new ones, delete them
                if prev_log_index + 1 < len(self.log):
                    self.log = self.log[:prev_log_index + 1]
                
                # Append new entries to log
                self.log.extend(new_entries)
                print(f"Node {self.node_id} appended {len(new_entries)} entries to log")
            
            # Update commitIndex if leader's commit index is higher
            if leader_commit > self.commit_index:
                old_commit = self.commit_index
                self.commit_index = min(leader_commit, len(self.log) - 1)
                print(f"Node {self.node_id} updated commit_index from {old_commit} to {self.commit_index}")
            
            return {"term": self.current_term, "success": True}
    
    # Client API Methods
    def get_status(self):
        """Return the current status of this node"""
        with self.state_lock:
            return {
                "role": self.role,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied
            }
    
    def create_topic(self, topic):
        """Create a new topic if it doesn't exist"""
        with self.state_lock:
            # Only the leader can modify the log
            if self.role != LEADER:
                return {"success": False, "leader_id": self.leader_id}
            
            # Check if topic already exists
            with self.apply_lock:
                if topic in self.topics:
                    return {"success": False, "error": "Topic already exists"}
            
            # Create a log entry
            entry = LogEntry(
                term=self.current_term,
                operation=OP_CREATE_TOPIC,
                data={"topic": topic}
            )
            
            # Append to log
            self.log.append(entry)
            log_index = len(self.log) - 1
            print(f"Node {self.node_id} added CREATE_TOPIC entry at index {log_index}")
            
            # Try to replicate immediately
            self._replicate_logs_to_all()
            
            return {"success": True}
    
    def get_topics(self):
        """Get all available topics"""
        with self.state_lock:
            # Only the leader can serve data
            if self.role != LEADER:
                return {"success": False, "leader_id": self.leader_id}
            
            with self.apply_lock:
                return {"success": True, "topics": self.topics}
    
    def put_message(self, topic, message):
        """Add a message to the specified topic queue"""
        with self.state_lock:
            # Only the leader can modify the log
            if self.role != LEADER:
                return {"success": False, "leader_id": self.leader_id}
            
            # Check if topic exists
            with self.apply_lock:
                if topic not in self.topics:
                    return {"success": False, "error": "Topic does not exist"}
            
            # Create a log entry
            entry = LogEntry(
                term=self.current_term,
                operation=OP_PUT_MESSAGE,
                data={"topic": topic, "message": message}
            )
            
            # Append to log
            self.log.append(entry)
            log_index = len(self.log) - 1
            print(f"Node {self.node_id} added PUT_MESSAGE entry at index {log_index}")
            
            # Try to replicate immediately
            self._replicate_logs_to_all()
            
            return {"success": True}
    
    def get_message(self, topic):
        """Get and remove the oldest message from the specified topic"""
        with self.state_lock:
            # Only the leader can modify the log
            if self.role != LEADER:
                return {"success": False, "leader_id": self.leader_id}
            
            # Check if topic exists and has messages
            with self.apply_lock:
                if topic not in self.topics:
                    return {"success": False, "error": "Topic does not exist"}
                
                if not self.messages[topic]:
                    return {"success": False, "error": "No messages in topic"}
                
                # Peek at the next message
                message = self.messages[topic][0]
            
            # Create a log entry for the GET operation
            entry = LogEntry(
                term=self.current_term,
                operation=OP_GET_MESSAGE,
                data={"topic": topic}
            )
            
            # Append to log
            self.log.append(entry)
            log_index = len(self.log) - 1
            print(f"Node {self.node_id} added GET_MESSAGE entry at index {log_index}")
            
            # Try to replicate immediately
            self._replicate_logs_to_all()
            
            # Return the message (it will be removed when the log entry is applied)
            return {"success": True, "message": message}

# Flask application
app = Flask(__name__)
node = None

@app.route('/', methods=['GET'])
def index():
    return "Raft Message Queue Node is running"

@app.route('/status', methods=['GET'])
def status():
    return jsonify(node.get_status())

@app.route('/topic', methods=['GET'])
def get_topics():
    return jsonify(node.get_topics())

@app.route('/topic', methods=['PUT'])
def create_topic():
    data = request.json
    if not data or 'topic' not in data:
        return jsonify({"success": False, "error": "Missing topic parameter"}), 400
    
    return jsonify(node.create_topic(data['topic']))

@app.route('/message', methods=['PUT'])
def put_message():
    data = request.json
    if not data or 'topic' not in data or 'message' not in data:
        return jsonify({"success": False, "error": "Missing required parameters"}), 400
    
    return jsonify(node.put_message(data['topic'], data['message']))

@app.route('/message/<topic>', methods=['GET'])
def get_message(topic):
    return jsonify(node.get_message(topic))

@app.route('/request_vote', methods=['POST'])
def request_vote():
    data = request.json
    if not data or 'term' not in data or 'candidate_id' not in data:
        return jsonify({"success": False, "error": "Missing required parameters"}), 400
    
    last_log_index = data.get('last_log_index', -1)
    last_log_term = data.get('last_log_term', 0)
    
    return jsonify(node.handle_request_vote(data['term'], data['candidate_id'], last_log_index, last_log_term))

@app.route('/append_entries', methods=['POST'])
def append_entries():
    data = request.json
    if not data or 'term' not in data or 'leader_id' not in data:
        return jsonify({"success": False, "error": "Missing required parameters"}), 400
    
    prev_log_index = data.get('prev_log_index', -1)
    prev_log_term = data.get('prev_log_term', 0)
    entries = data.get('entries', [])
    leader_commit = data.get('leader_commit', -1)
    
    return jsonify(node.handle_append_entries(
        data['term'], data['leader_id'], prev_log_index, prev_log_term, entries, leader_commit
    ))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python node.py <config_path> <node_id>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    node_id = sys.argv[2]
    
    # Initialize node
    node = RaftNode(config_path, node_id)
    
    # Get address information
    host = node.address["ip"]
    port = node.address["port"]
    
    # Extract the host from the IP (removing the http:// prefix)
    if host.startswith("http://"):
        host = host[7:]  # Remove 'http://' prefix
    
    # Start Flask application
    app.run(host=host, port=port, threaded=True)