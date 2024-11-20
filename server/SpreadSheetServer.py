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

FINGER_NUM  = 16
MAX_KEY     = 2 ** FINGER_NUM

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

def print_info(server): # print connection infos every 5 sec
    while True:
        print(f"node_id: {server.node_id}")
        if server.successor: print(f"successor: {server.successor.host}:{server.successor.port}, {server.successor.node_id}")
        if server.predecessor: print(f"predecessor: {server.predecessor.host}:{server.predecessor.port}, {server.predecessor.node_id}")
        print("\n\tfinger_table: ")
        for target_id, node_id, host, port, socket in server.finger_table:
            print(f'{target_id}\t{node_id}\t: {host}:{port}')
        print("\n\n")
        time.sleep(5)

class Node:
    def __init__(self, host, port, node_id, sock=None):
        self.host = host
        self.port = int(port)
        self.node_id = int(node_id) % MAX_KEY
        if sock: 
            self.socket = sock
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))


class SpreadSheetServer:
    def __init__(self, project_name, node_id, host, port):
        self.node_id = int(node_id) % MAX_KEY
        self.host = host    # master host and port
        self.port = port
        self.client_sockets = {}
        self.spreadsheet = SpreadSheet(node_id=self.node_id)
        self.project_name = f'{project_name}_{node_id}' 
        self.successor = None
        self.predecessor = None     
        self.finger_table = [[(self.node_id + 2**i) % MAX_KEY, None, None, None, None] for i in range(FINGER_NUM)]      # [[target_id, node_id, node_host, node_port, socket]]
        for i in range(FINGER_NUM):
            self.finger_table[i][1], self.finger_table[i][2], self.finger_table[i][3], self.finger_table[i][4] = self.node_id, self.host, self.port, None
        self._join()
        

    def _join(self):
        """ New node tries to join existing chord system """
        try:
            # connect to a random server, and send join request
            response = requests.get("http://catalog.cse.nd.edu:9097/query.json")    # name server
            services = response.json()
            # TODO: retry connecting to service (loop through all possible names)
            service = max([service for service in services if service.get("type") == "spreadsheet" and service.get("project").split('_')[0] == self.project_name.split('_')[0]], key=lambda x: x.get("lastheardfrom"))
            random_host = service.get("name")
            random_port = service.get("port")
            join_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            join_socket.connect((random_host, random_port))
            response_data = self.send_request(join_socket, {"method": "join"})  # get successor addr from response
            join_socket.close()

            self.successor = Node(response_data["host"], response_data["port"], response_data["node_id"])   # set successor and connect
            # update finger table to include successor
            self.update_finger_table(self.successor.node_id, self.successor.host, self.successor.port)
            print(f'sucessor connected: {self.successor.host, self.successor.port, self.successor.node_id}')
            
            self.send_message(self.successor.socket, {"method": "imYourPred", "host": self.host, "port": self.port, "node_id": self.node_id}) # inform successor of its pred

        except Exception as e:
            print(e)
            print('first server')
            # for i in range(FINGER_NUM):
            #     self.finger_table[i][1], self.finger_table[i][2], self.finger_table[i][3], self.finger_table[i][4] = self.node_id, self.host, self.port, None

    def _establish_chord(self):

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
            print(f'received response: {response_data}')
            return json.loads(response_data.decode('utf-8').strip())
        except Exception as e:
            print(f"Request: {request}\n Error: {e}\n")
    
    def send_message(self, socket, message):
        try:
            print(f'sending message: {message}')
            message_data = f'{json.dumps(message)}\n'.encode('utf-8')
            socket.sendall(message_data)
            
        except Exception as e:
            print(f"message: {message}\n Error: {e}\n")

    def handle_request(self, request, socket):
        try:
            method = request.get("method")
            # key = int(sha256(f"{key},{col}".encode()).hexdigest(), 16) % 2**32  # Calculate key

            # if self.find_successor(key) != self.node_id:
            #     # Route request to the responsible node (using an RPC call)
            #     successor = self.find_successor(key)
            #     self.forward_request_to_successor(successor, request)
            #     return {"status": "forwarded", "node": successor}

            # If the node is responsible, handle the request
            if method == "insert":
                key = request.get("key")
                return self.spreadsheet.insert(key, request["value"])
            elif method == "lookup":
                key = request.get("key")
                return self.spreadsheet.lookup(key)
            elif method == "remove":
                key = request.get("key")
                return self.spreadsheet.remove(key)
            elif method == "join":
                # TODO: route, return port + host of successor
                return {"status": "success", "host": f"{self.host}", "port": f"{self.port}", "node_id": f"{self.node_id}"}
            elif method == "imYourPred":
                pred_host, pred_port = request.get("host"), request.get("port")

                if not self.predecessor:    # new node or node1 receiving
                    if not self.successor:  # node1 receiving
                        self.predecessor = Node(pred_host, pred_port, request.get("node_id"), socket)
                        self.successor = self.predecessor
                        self.send_message(self.successor.socket, {"method": "imYourPred", "host": self.host, "port": self.port, "node_id": self.node_id})
                    else:   # new node receives
                        self.predecessor = Node(pred_host, pred_port, request.get("node_id"), socket)
                        # update finger table to include predecessor
                        self.update_finger_table(self.predecessor.node_id, self.predecessor.host, self.predecessor.port)
                else:   # ring member receives
                    # inform predecessor
                    self.send_message(self.predecessor.socket, {"method": "yourNewSucc", "host": pred_host, "port": pred_port, "node_id": request.get("node_id")})
                    self.predecessor = Node(pred_host, pred_port, request.get("node_id"), socket)
                    # TODO: notify all its PT of new pred's existance

                    # TODO: help predecessor to establish finger table
                    # for i in range(FINGER_NUM):
                    #     target_id = (self.predecessor.node_id + 2**i) % MAX_KEY
                    #     route_target = self._route(target_id)
                    #     print(f'{target_id} routes to {route_target}')

                # update finger table to include predecessor
                self.update_finger_table(self.predecessor.node_id, self.predecessor.host, self.predecessor.port)

                        
            elif method == "yourNewSucc":
                succ_host, succ_port = request.get("host"), request.get("port")
                self.successor = Node(succ_host, succ_port, request.get("node_id"))
                # update finger table to include successor
                self.update_finger_table(self.successor.node_id, self.successor.host, self.successor.port)
                self.send_message(self.successor.socket, {"method": "imYourPred", "host": self.host, "port": self.port, "node_id": self.node_id})
            else:
                # return {"status": "error", "message": f"Invalid method: {method}. (insert/lookup/remove)"}
                pass
        except Exception as e:
            print("error in handling request")
            print(e)
            # return {"status": "error", "message": f"Invalid request {request}; method required"}
    
    def _inInterval(self, start, end, val):
        """ test if val is in (start, end] in the chord """
        if start <= end:
            return start < val <= end
        return not (end < val <= start)

    def _route(self, target_id):
        """ route target_id based on finger table """
        for i in range(FINGER_NUM):
            last = self.finger_table[i-1][1]
            curr = self.finger_table[i][1]
            if self._inInterval(last, curr, target_id):
                return self.finger_table[i]

    def update_finger_table(self, joining_node_id, joining_host, joining_port):
        """ Update the finger table entries when a new node joins. """
        for i in range(FINGER_NUM):
            if self._inInterval(self.finger_table[i][0], self.finger_table[i][1], joining_node_id):
                self.finger_table[i][1:] = joining_node_id, joining_host, joining_port, None
                

                

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
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.bind(('', 0))
        master_socket.listen(5)
        server = SpreadSheetServer(project_name, node_id, socket.getfqdn(), master_socket.getsockname()[1])
        print(f"Listening on port {server.port}")
        if server.successor:
            print(server.successor.node_id)
        # Background thread to register with the name server
        threading.Thread(target=register_name_server, args=(server.port, f'{project_name}_{node_id}'), daemon=True).start()
        threading.Thread(target=print_info, args=(server,), daemon=True).start()

        server.client_sockets = {}
        while True:
            if server.successor and server.successor.socket not in server.client_sockets:
                server.client_sockets[server.successor.socket] = (server.successor.host, server.successor.port)
            if server.predecessor and server.predecessor.socket not in server.client_sockets:
                server.client_sockets[server.predecessor.socket] = (server.predecessor.host, server.predecessor.port)

            sockets_to_read = [master_socket] + list(server.client_sockets.keys())

            readable_sockets, _, _ = select.select(sockets_to_read, [], [])

            for sock in readable_sockets:
                if sock is master_socket:  # new connection
                    client_socket, addr = master_socket.accept()
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
                        if response:
                            response_data = f'{json.dumps(response)}\n'.encode('utf-8')
                            sock.sendall(response_data)  # send response

                    except EOFError:
                        print(f"{sock.getpeername()} disconnected")
                        sock.close()
                        del server.client_sockets[sock]
                    except (ConnectionResetError, BrokenPipeError) as e:
                        print(f"{server.client_sockets[sock]} disconnected unexpectedly: {e}")
                    except json.JSONDecodeError:
                        print(f"Received malformed JSON from {server.client_sockets[sock]}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 SpreadSheetServer.py <project_name> <node_id>")
        sys.exit(1)
    start_server(sys.argv[1], sys.argv[2])