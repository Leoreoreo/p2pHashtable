import json
from hashlib import sha256

class Node:
    def __init__(self, name):
        self.name = name
        self.node_id = int(sha256(name.encode()).hexdigest(), 16) % 2**32  # Unique ID based on the node name
        self.successor = self.node_id  # Initially set successor to itself
        self.predecessor = None  # Initially, the predecessor is None
        self.finger_table = {}  # Finger table for efficient routing
        print(f"Node {self.name} initialized with ID {self.node_id}")

    def set_successor(self, successor_node):
        """Set the current node's successor"""
        self.successor = successor_node.node_id
        print(f"Node {self.name}'s successor set to Node {successor_node.name}")

    def set_predecessor(self, predecessor_node):
        """Set the current node's predecessor"""
        self.predecessor = predecessor_node.node_id
        print(f"Node {self.name}'s predecessor set to Node {predecessor_node.name}")

    def _populate_finger_table(self, all_nodes):
        """Populate the finger table for efficient routing"""
        for i in range(32):  # Assuming a 32-bit hash space
            finger_id = (self.node_id + 2**i) % 2**32
            self.finger_table[i] = self.find_successor(finger_id, all_nodes)

    def find_successor(self, key, all_nodes):
        """Find the successor node responsible for a given key"""
        sorted_nodes = sorted(all_nodes, key=lambda x: x.node_id)
        for node in sorted_nodes:
            if node.node_id >= key:
                return node.node_id
        return sorted_nodes[0].node_id  # Return the ID of the first node (circular structure)

    def display_info(self):
        """Display basic information about the node"""
        print(f"Node {self.name}:")
        print(f"  ID: {self.node_id}")
        print(f"  Successor: {self.successor}")
        print(f"  Predecessor: {self.predecessor}")
        print("  Finger Table:")
        for i, fid in self.finger_table.items():
            print(f"    Entry {i}: Node ID {fid}")

# Create five nodes A, B, C, D, E
node_A = Node("A")
node_B = Node("B")
node_C = Node("C")
node_D = Node("D")
node_E = Node("E")

# Add nodes to a list for easy manipulation
all_nodes = [node_A, node_B, node_C, node_D, node_E]

# Set successors and predecessors to form a Chord ring
node_A.set_successor(node_B)
node_B.set_successor(node_C)
node_C.set_successor(node_D)
node_D.set_successor(node_E)
node_E.set_successor(node_A)  # Wraps around to A

node_A.set_predecessor(node_E)
node_B.set_predecessor(node_A)
node_C.set_predecessor(node_B)
node_D.set_predecessor(node_C)
node_E.set_predecessor(node_D)

# Populate the finger table for each node
for node in all_nodes:
    node._populate_finger_table(all_nodes)

# Display information about each node
for node in all_nodes:
    node.display_info()