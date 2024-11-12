# SpreadSheetServer

import socket
import json
import sys
import time
import threading
import os
from SpreadSheet import SpreadSheet
import select
from hashlib import sha256

def register_name_server(port, project_name):
    name_server_address = ("catalog.cse.nd.edu", 9097)
    while True:
        message = {
            "type": "spreadsheet",
            "owner": "lli27", # Owner ID
            "port": port,
            "project": project_name,
            "width": 120,
            "height": 16
        }
        # Send UDP packet to the name server
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            udp_socket.sendto(json.dumps(message).encode(), name_server_address)
        # register once a minute
        time.sleep(60)

class SpreadSheetServer:
    def __init__(self, project_name):
        self.spreadsheet = SpreadSheet()
        self.node_id = int(sha256(project_name.encode()).hexdigest(), 16) % 2**32  # Generate node ID
        self.successor = None  # Node ID of the successor
        self.predecessor = None  # Node ID of the predecessor
        self.finger_table = {}  # Finger table for routing
        self.project_name = project_name
        self._initialize_chord()

    def _initialize_chord(self):
        """ Initialize Chord-specific parameters, setting successor and populating the finger table. """
        self.successor = self.node_id  # Initially, set successor to itself
        self._populate_finger_table()

    def _populate_finger_table(self):
        """ Populate the finger table based on Chord's finger table logic. """
        for i in range(32):  # Assuming a 32-bit hash space
            finger_id = (self.node_id + 2**i) % 2**32
            self.finger_table[i] = self.find_successor(finger_id)

    def find_successor(self, key):
        """ Find the successor node responsible for a given key. """
        if self.node_id < key <= self.successor:
            return self.successor
        # Traverse finger table in reverse to find the closest preceding node
        for i in reversed(range(32)):
            finger_id = self.finger_table[i]
            if self.node_id < finger_id < key:
                return finger_id
        return self.successor

    def update_finger_table(self, joining_node_id):
        """ Update the finger table entries when a new node joins. """
        for i in range(32):
            finger_id = (self.node_id + 2**i) % 2**32
            if self.node_id < finger_id <= joining_node_id:
                self.finger_table[i] = joining_node_id

    def handle_request(self, request):
        try:
            method = request.get("method")
            row, col = request.get("row"), request.get("column")
            key = int(sha256(f"{row},{col}".encode()).hexdigest(), 16) % 2**32  # Calculate key

            if self.find_successor(key) != self.node_id:
                # Route request to the responsible node (using an RPC call)
                successor = self.find_successor(key)
                self.forward_request_to_successor(successor, request)
                return {"status": "forwarded", "node": successor}

            # If the node is responsible, handle the request
            if method == "insert":
                return self.spreadsheet.insert(row, col, request["value"])
            elif method == "lookup":
                return self.spreadsheet.lookup(row, col)
            elif method == "remove":
                return self.spreadsheet.remove(row, col)
            else:
                return {"status": "error", "message": "Invalid method. (insert/lookup/remove)"}
        except:
            return {"status": "error", "message": "Invalid request; method required"}

    def join(self, existing_node_id):
        """ Join an existing Chord ring. """
        if existing_node_id != self.node_id:
            self.successor = self.find_successor(existing_node_id)
            self.update_finger_table(existing_node_id)

    def leave(self):
        """ Leave the Chord ring, transferring data and updating neighbors. """
        if self.successor:
            # Transfer all data to the successor before leaving
            self.transfer_data_to_successor(self.successor)
            # Notify successor and predecessor of departure

    def transfer_data_to_successor(self, successor_id):
        """ Transfer responsibility of keys to the successor. """
        transferred_data = {key: value for key, value in self.spreadsheet.data.items() if key <= successor_id}
        # Logic to transfer data to the successor (e.g., send via network)

def start_server(project_name):
    server = SpreadSheetServer(project_name)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.bind(('', 0))
        master_socket.listen(5)
        port = master_socket.getsockname()[1]
        print(f"Listening on port {port}")

        # Background thread to register with the name server
        threading.Thread(target=register_name_server, args=(port, project_name), daemon=True).start()

        client_sockets = {}
        while True:
            sockets_to_read = [master_socket] + list(client_sockets.keys())
            readable_sockets, _, _ = select.select(sockets_to_read, [], [])

            for sock in readable_sockets:
                if sock is master_socket:  # new connection
                    client_socket, addr = master_socket.accept()
                    print(f"New connection from {addr}")
                    client_sockets[client_socket] = addr
                else:
                    try:
                        data = b''
                        while not data.endswith(b'\n'):
                            more = sock.recv(1)
                            if not more:
                                raise EOFError("Client has closed the connection.")
                            data += more
                        data = data.decode('utf-8').strip()

                        print(client_sockets[sock], data)

                        request = json.loads(data)
                        response = server.handle_request(request)
                        response_data = f'{json.dumps(response)}\n'.encode('utf-8')
                        sock.sendall(response_data)  # send response

                    except EOFError:
                        print(f"Client {sock.getpeername()} disconnected")
                        sock.close()
                        del client_sockets[sock]
                    except (ConnectionResetError, BrokenPipeError) as e:
                        print(f"Client {client_sockets[sock]} disconnected unexpectedly: {e}")
                    except json.JSONDecodeError:
                        print(f"Received malformed JSON from {client_sockets[sock]}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 SpreadSheetServer.py <project_name>")
        sys.exit(1)
    start_server(sys.argv[1])