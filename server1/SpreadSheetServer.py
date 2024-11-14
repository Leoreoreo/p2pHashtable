# SpreadSheetServer

import socket
import json
import sys
import time
import threading
import os
from SpreadSheet import SpreadSheet
import select
import requests

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

class Node:
    def __init__(self, host, port, node_id, sock=None):
        self.host = host
        self.port = int(port)
        self.node_id = int(node_id)
        if not sock:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
        else:
            self.socket = sock


class SpreadSheetServer:
    def __init__(self, project_name, node_id):
        self.spreadsheet = SpreadSheet()
        self.client_sockets = {}
        self.node_id = node_id
        self.master_socket = None
        self.host = None
        self.port = None
        self.successor = None       
        self.predecessor = None     
        self.finger_table = {}      # Finger table for routing
        self.project_name = f'{project_name}_{node_id}'

    def join(self):
        """ New node tries to join existing chord system """
        try:
            response = requests.get("http://catalog.cse.nd.edu:9097/query.json")    # name server
            services = response.json()
            service = max([service for service in services if service.get("type") == "spreadsheet" and service.get("project").split('_')[0] == self.project_name.split('_')[0]], key=lambda x: x.get("lastheardfrom"))
            self.host = service.get("name")
            self.port = service.get("port")
            print(f'contacting to: {self.host, self.port} for join request')

            join_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            join_socket.connect((self.host, self.port))
            response_data = self.send_request(join_socket, {"method": "join"})
            join_socket.close()
            self.successor = Node(response_data["host"], response_data["port"], response_data["node_id"])
            print(f'sucessor connected: {self.successor.host, self.successor.port, self.successor.node_id}')

            self.send_request(self.successor.socket, {"method": "imYourPred", "node_id": self.node_id})

        except Exception as e:
            print(e)
            print('first server, initialize chord')
            self._initialize_chord()

    def _initialize_chord(self):
        """ Initialize Chord-specific parameters, setting successor and populating the finger table. """
        pass
    # def _populate_finger_table(self):
    #     """ Populate the finger table based on Chord's finger table logic. """
    #     for i in range(32):  # Assuming a 32-bit hash space
    #         finger_id = (self.node_id + 2**i) % 2**32
    #         self.finger_table[i] = self.find_successor(finger_id)

    # def find_successor(self, key):
    #     """ Find the successor node responsible for a given key. """
    #     if self.node_id < key <= self.successor:
    #         return self.successor
    #     # Traverse finger table in reverse to find the closest preceding node
    #     for i in reversed(range(32)):
    #         finger_id = self.finger_table[i]
    #         if self.node_id < finger_id < key:
    #             return finger_id
    #     return self.successor

    def update_finger_table(self, joining_node_id):
        """ Update the finger table entries when a new node joins. """
        for i in range(32):
            finger_id = (self.node_id + 2**i) % 2**32
            if self.node_id < finger_id <= joining_node_id:
                self.finger_table[i] = joining_node_id

    def send_request(self, socket, request):
        try:
            print(f'sending request: {request}')
            request_data = f'{json.dumps(request)}\n'.encode('utf-8')
            socket.sendall(request_data)
            socket.settimeout(5)             # wait at most 5 sec

            response_data = b''
            while not response_data.endswith(b'\n'):
                more = socket.recv(1)
                if not more:
                    raise EOFError("Socket connection broken")
                response_data += more
            return json.loads(response_data.decode('utf-8').strip())
        except Exception as e:
            print(f"Request: {request}\n Error: {e}\n")

    def handle_request(self, request, socket):
        try:
            method = request.get("method")
            # key = int(sha256(f"{row},{col}".encode()).hexdigest(), 16) % 2**32  # Calculate key

            # if self.find_successor(key) != self.node_id:
            #     # Route request to the responsible node (using an RPC call)
            #     successor = self.find_successor(key)
            #     self.forward_request_to_successor(successor, request)
            #     return {"status": "forwarded", "node": successor}

            # If the node is responsible, handle the request
            if method == "insert":
                row, col = request.get("row"), request.get("column")
                return self.spreadsheet.insert(row, col, request["value"])
            elif method == "lookup":
                row, col = request.get("row"), request.get("column")
                return self.spreadsheet.lookup(row, col)
            elif method == "remove":
                row, col = request.get("row"), request.get("column")
                return self.spreadsheet.remove(row, col)
            elif method == "join":
                # TODO: route, return port + host of successor
                return {"status": "success", "host": f"{self.host}", "port": f"{self.port}", "node_id": f"{self.node_id}"}
            elif method == "imYourPred":
                pred_host, pred_port = socket.getpeername()
                pred_socket = None
                for sock, addr in self.client_sockets.items():
                    if addr == (pred_host, pred_port):
                        pred_socket = sock
                if not self.predecessor:
                    self.predecessor = Node(pred_host, pred_port, request.get("node_id"), pred_socket)
                    
                    self.successor = Node(pred_host, pred_port, request.get("node_id"), pred_socket)
                    # self.send_request(pred_socket, {"method": "imYourPred", "node_id": self.node_id})
                    print("successor: ", self.successor.host, self.successor.port, self.successor.node_id)
                    print("predecessor: ", self.predecessor.host, self.predecessor.port, self.predecessor.node_id)
                else:
                    # TODO inform predecessor
                    send_request(self.predecessor.socket, {"method": "yourNewSucc", "host": pred_host, "port": pred_port, "node_id": request.get("node_id")})
                    self.predecessor = Node(pred_host, pred_port, request.get("node_id"), pred_socket)
                print(f'predecessor connected: {self.predecessor.host, self.predecessor.port, self.predecessor.node_id}')
                return {"status": "success"}
            elif method == "yourNewSucc":
                # succ_host, succ_port = request.get()
                pass
            else:
                return {"status": "error", "message": f"Invalid method: {method}. (insert/lookup/remove)"}
        except:
            return {"status": "error", "message": "Invalid request; method required"}

    # def join(self, existing_node_id):
    #     """ Join an existing Chord ring. """
    #     if existing_node_id != self.node_id:
    #         self.successor = self.find_successor(existing_node_id)
    #         self.update_finger_table(existing_node_id)

    # def leave(self):
    #     """ Leave the Chord ring, transferring data and updating neighbors. """
    #     if self.successor:
    #         # Transfer all data to the successor before leaving
    #         self.transfer_data_to_successor(self.successor)
    #         # Notify successor and predecessor of departure

    # def transfer_data_to_successor(self, successor_id):
    #     """ Transfer responsibility of keys to the successor. """
    #     transferred_data = {key: value for key, value in self.spreadsheet.data.items() if key <= successor_id}
    #     # Logic to transfer data to the successor (e.g., send via network)

def start_server(project_name, node_id):
    server = SpreadSheetServer(project_name, node_id)

    server.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.master_socket.bind(('', 0))
    server.master_socket.listen(5)
    server.host = socket.getfqdn()
    server.port = server.master_socket.getsockname()[1]
    server.join()
    print(f"Listening on port {server.port}")

    # Background thread to register with the name server
    threading.Thread(target=register_name_server, args=(server.port, f'{project_name}_{node_id}'), daemon=True).start()

    server.client_sockets = {}
    while True:
        if server.successor and server.successor.socket not in server.client_sockets:
            server.client_sockets[server.successor.socket] = (server.successor.host, server.successor.port)
        if server.predecessor and server.predecessor.socket not in server.client_sockets:
            server.client_sockets[server.predecessor.socket] = (server.predecessor.host, server.predecessor.port)

        sockets_to_read = [server.master_socket] + list(server.client_sockets.keys())

        readable_sockets, _, _ = select.select(sockets_to_read, [], [])

        for sock in readable_sockets:
            if sock is server.master_socket:  # new connection
                client_socket, addr = server.master_socket.accept()
                print(f"New connection from {addr}")
                server.client_sockets[client_socket] = addr
            else:
                try:
                    data = b''
                    while not data.endswith(b'\n'):
                        more = sock.recv(1)
                        if not more:
                            raise EOFError("Client has closed the connection.")
                        data += more
                    data = data.decode('utf-8').strip()

                    print(server.client_sockets[sock], data)

                    request = json.loads(data)
                    response = server.handle_request(request, sock)
                    response_data = f'{json.dumps(response)}\n'.encode('utf-8')
                    sock.sendall(response_data)  # send response

                except EOFError:
                    print(f"Client {sock.getpeername()} disconnected")
                    sock.close()
                    del server.client_sockets[sock]
                except (ConnectionResetError, BrokenPipeError) as e:
                    print(f"Client {server.client_sockets[sock]} disconnected unexpectedly: {e}")
                except json.JSONDecodeError:
                    print(f"Received malformed JSON from {server.client_sockets[sock]}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 SpreadSheetServer.py <project_name> <node_id>")
        sys.exit(1)
    start_server(sys.argv[1], sys.argv[2])