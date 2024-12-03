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

# ---------------------------------globals---------------------------------
FINGER_NUM  = 16
MAX_KEY     = 2 ** FINGER_NUM

# ------------------------background thread functions----------------------

def register_name_server(port, project_name):
    """ register to name server once a minute """
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

def print_info(server): 
    """ print connection infos every 5 sec """
    while True:
        print(f"\nnode_id: {server.node_id}")
        size = len(server.spreadsheet.data)
        resp = len([key for key in server.spreadsheet.data.keys() if server._isResponsible(key)])
        print(f"data size: {size}\t resp: {resp}\t repl: {size-resp}")
        # print(f"resp: {[key for key in server.spreadsheet.data.keys() if server._isResponsible(key)]}")
        # print(f"repl: {[key for key in server.spreadsheet.data.keys() if not server._isResponsible(key)]}")

        if server.predecessor: print(f"predecessor: {server.predecessor.host}:{server.predecessor.port}, {server.predecessor.node_id}")
        if server.successor: print(f"successor: {server.successor.host}:{server.successor.port}, {server.successor.node_id}")
        
        print(f'\n\tfinger_table ({server.node_id}): ')
        for target_id, node_id, host, port, socket in server.finger_table:
            print(f'{target_id}\t{node_id}\t: {host}:{port}, {"con" if socket else "not"}')
        # print(f'\n\tpointed_table ({server.node_id}): ')
        # for node_id, row in server.pointed_table.items():
        #     print(f'{node_id}\t{row[:-1]}')
        # print(f'\n\tpred_finger_table ({server.predecessor.node_id if server.predecessor else None}): ')
        # for target_id, node_id, host, port in server.pred_finger_table:
        #     print(f'{target_id}\t{node_id}\t: {host}:{port}')
        # print(f'\n\tpred_pointed_table ({server.predecessor.node_id if server.predecessor else None}): ')
        # for node_id, row in server.pred_pointed_table.items():
        #     print(f'{node_id}\t: {row}')
        print("\n")
        time.sleep(5)

# ---------------------------------Classes---------------------------------
class Node:
    """ Node: used by SpreadSheetServer's predecessor and successor"""
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
    """ SpreadSheetServer: the server class """
    def __init__(self, project_name, node_id, host, port):
        self.node_id = int(node_id) % MAX_KEY
        self.project_name = f'{project_name}_{node_id}' 

        self.host = host    # master host and port
        self.port = port
        
        self.client_sockets = {}    # all other sockets connected
        self.spreadsheet = SpreadSheet(node_id=self.node_id)    # where spreadsheet data and operations stored

        self.successor = None
        self.predecessor = None 

        self.finger_table = [[(self.node_id + 2**i) % MAX_KEY, self.node_id, self.host, self.port, None] for i in range(FINGER_NUM)]      # [[target_id, node_id, node_host, node_port, socket]]
        self.pointed_table = {}         # {node_id: [count, node_host, node_port, socket]}

        self.pred_finger_table = []     # [[target_id, node_id, node_host, node_port]]
        self.pred_pointed_table = {}    # {node_id: [count, node_host, node_port]}
        
        self.message_dic = {}       # stores incoming messages: {msg_id: (source_sock, target_sock)}
        self.msg_counter = 0        # self unique msg_id counter

        self._join()    # call join() to join the chord

        self.flag = 0   # flag = 0: predecessor is stable; flag = 1: predecessor is unstable
        self.last_pred_id = None
    

    def _join(self):
        """ New node tries to join existing chord system """
        try:
            # connect to a random server, and send join request
            response = requests.get("http://catalog.cse.nd.edu:9097/query.json")    # connect to name server
            services = response.json()

            # select a random server
            # retry connecting to service (loop through all possible names)
            for service in [service for service in services if service.get("type") == "spreadsheet" and service.get("project").split('_')[0] == self.project_name.split('_')[0]]:
                try:
                    random_host = service.get("name")
                    random_port = service.get("port")
                    join_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    join_socket.connect((random_host, random_port)) # connect to this server, and send join request
                    break
                except:
                    pass
            response_data = self.send_request(join_socket, {"method": "join", "key": self.node_id})  # get successor addr from response
            join_socket.close()

            # connect to successor
            self.successor = Node(response_data["host"], response_data["port"], response_data["node_id"])   # set successor and connect
            # update finger table to include successor
            self.update_finger_table(self.successor.node_id, self.successor.host, self.successor.port, False)
            print(f'successor connected: {self.successor.host, self.successor.port, self.successor.node_id}')
            
            self.send_message(self.successor.socket, {"method": "imYourPred", "host": self.host, "port": self.port, "node_id": self.node_id}) # inform successor of its pred

        except Exception as e:
            print(e)
            print('first server')


    def _establish_chord(self):
        """ establish finger table """
        print("establishing finger table")
        affected_sockets = set()
        for i in range(FINGER_NUM):
            response_data = self.send_request(self.successor.socket, {"method": "establishChord", "key": self.finger_table[i][0]})
            if "node_id" not in response_data:  # route back to itself => it is responsible
                continue
            try:
                node_id = int(response_data["node_id"])
                host = response_data["host"]
                port = int(response_data["port"])
                if self.finger_table[i-1][1] == node_id:    # check if match the same node
                    self.finger_table[i][1:] = self.finger_table[i-1][1:]
                    self.send_message(self.finger_table[i][-1], {"method": "imPointingAtYou", "host": self.host, "port": self.port, "node_id": self.node_id})
                else:
                    exist = False
                    for sock, addr in self.client_sockets.items():  # check if already in socket list
                        if addr[0] == host and addr[1] == port:
                            self.finger_table[i][1:] = node_id, host, port, sock
                            affected_sockets.add(sock)
                            self.send_message(sock, {"method": "imPointingAtYou", "host": self.host, "port": self.port, "node_id": self.node_id})
                            exist = True
                            break
                    if not exist:
                        finger_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        finger_socket.connect((host, port))
                        affected_sockets.add(finger_socket)
                        self.finger_table[i][1:] = node_id, host, port, finger_socket
                        self.send_message(finger_socket, {"method": "imPointingAtYou", "host": self.host, "port": self.port, "node_id": self.node_id})
            except Exception as e:
                print(f"Error establishing chord: {e}")
        # inform successor to updatePFT
        self.send_message(self.successor.socket, {"method": "updatePFT", "PFT": [row[:-1] for row in self.finger_table]})
        for sock in affected_sockets | {self.successor.socket}:
            self.send_message(sock, {"method": "chordEstablishmentCompleted"})
        self.send_message(self.successor.socket, {"method": "readyForDataTransfer"})

    def handle_pred_failure(self):
        """ handle predecessor failure """
        # inform all nodes pointed by my predecessor that they are no longer pointed
        for target_id, node_id, host, port in self.pred_finger_table:
            if node_id == self.node_id:
                self.pointed_table[self.predecessor.node_id][0] -= 1
                if self.pointed_table[self.predecessor.node_id][0] == 0:
                    del self.pointed_table[self.predecessor.node_id]
                continue
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            self.send_message(sock, {"method": "imNotPointingAtYou", "node_id": self.predecessor.node_id})
            sock.close()
        # inform all nodes pointing at my predecessor that I'm taking over
        for node_id, row in self.pred_pointed_table.items():
            _, host, port = row
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            self.flag += 1
            self.send_message(sock, {"method": "takeover", "old_id": self.predecessor.node_id, "new_id": self.node_id, "host": self.host, "port": self.port})
            sock.close()
        

    def send_request(self, socket, request):
        """ send request (returns response) """
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
        """ send message (returns nothing) """
        try:
            print(f'sending message: {message}')
            message_data = f'{json.dumps(message)}\n'.encode('utf-8')
            socket.sendall(message_data)
            
        except Exception as e:
            print(f"message: {message}\n Error: {e}\n")

    def handle_request(self, request, sock):
        """ handle incoming messages / requests (returns nothing) """
        try:
            if "status" in request:     # response
                self.send_message(self.message_dic[request.get("msg_id")][0], request)
                del self.message_dic[request.get("msg_id")]

            else:                       # request
                method = request.get("method")

                # If the node is responsible, perform the request
                if "key" not in request or self._isResponsible(request.get("key")):  # perform the request, return result
                    # spreadsheet operations
                    if method == "insert":
                        key = request.get("key")
                        message = self.spreadsheet.insert(key, request["value"])
                        if request.get("msg_id"):
                            message["msg_id"] = request.get("msg_id")
                        self.send_message(sock, message)
                        self.send_message(self.successor.socket, {"method": "insert_replication", "repli_key": key, "value": request["value"]})
                    elif method == "insert_replication":
                        self.spreadsheet.insert(request["repli_key"], request["value"])
                    elif method == "lookup":
                        key = request.get("key")
                        message = self.spreadsheet.lookup(key)
                        if request.get("msg_id"):
                            message["msg_id"] = request.get("msg_id")
                        self.send_message(sock, message)
                    elif method == "remove":
                        key = request.get("key")
                        message = self.spreadsheet.remove(key)
                        if request.get("msg_id"):
                            message["msg_id"] = request.get("msg_id")
                        self.send_message(sock, message)
                        self.send_message(self.successor.socket, {"method": "remove_replication", "repli_key": key})
                    elif method == "remove_replication":
                        self.spreadsheet.remove(request["repli_key"])

                    # new node ask to join chord, the node happens to be its successor
                    elif method == "join":
                        message = {"status": "success", "host": f"{self.host}", "port": f"{self.port}", "node_id": f"{self.node_id}"}
                        if request.get("msg_id"):
                            message["msg_id"] = request.get("msg_id")
                        self.send_message(sock, message)

                    # needs to update finger table because old pointer is invalid
                    elif method == "takeover":
                        host = request.get("host")
                        port = request.get("port")
                        old_id = request.get("old_id")
                        new_id = request.get("new_id")
                        new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        new_sock.connect((host, port))
                        # update finger table
                        for i in range(FINGER_NUM):
                            if self.finger_table[i][1] == old_id:
                                self.finger_table[i][1:] = new_id, host, port, new_sock
                                self.send_message(new_sock, {"method": "imPointingAtYou", "node_id": self.node_id, "host": self.host, "port": self.port})
                        # update successor if needed
                        if self.successor.node_id == old_id:
                            self.successor = Node(host, port, new_id, new_sock)
                            # send its data to its successor => update its replication in successor
                            for key, val in self.spreadsheet.data.items():
                                if self._isResponsible(key):
                                    self.send_message(self.successor.socket, {"method": "insert_replication", "repli_key": key, "value": val})

                        self.send_message(new_sock, {"method": "flag"})     # inform takeover node it has completed finger table change
                        # send updated FT and PT info to successor
                        self.send_message(self.successor.socket, {"method": "imYourUpdatedPred", "node_id": self.node_id, "host": self.host, "port": self.port, "PPT": {node_id: row[:-1] for node_id, row in self.pointed_table.items()}, "PFT": [row[:-1] for row in self.finger_table]})
                    
                    elif method == "flag":  # an affected node finished updating finger table
                        self.flag -= 1 
                        if self.flag == 0:  # all nodes finished => stable
                            # send updated FT and PT info to successor
                            self.send_message(self.successor.socket, {"method": "imYourUpdatedPred", "node_id": self.node_id, "host": self.host, "port": self.port, "PPT": {node_id: row[:-1] for node_id, row in self.pointed_table.items()}, "PFT": [row[:-1] for row in self.finger_table]})

                    elif method == "imYourUpdatedPred":
                        self.predecessor = Node(request.get("host"), request.get("port"), request.get("node_id"), sock)
                        self.pred_finger_table = request.get("PFT")
                        self.pred_pointed_table = request.get("PPT")
                        # send its data to its successor => update its replication in successor
                        for key, val in self.spreadsheet.data.items():
                            if self._isResponsible(key):
                                self.send_message(self.successor.socket, {"method": "insert_replication", "repli_key": key, "value": val})

                    elif method == "imYourPred":
                        pred_host, pred_port = request.get("host"), request.get("port")

                        if not self.predecessor:    # new node or node1 receiving
                            if not self.successor:  # node1 receiving
                                self.predecessor = Node(pred_host, pred_port, request.get("node_id"), sock)
                                # update finger table to include predecessor
                                self.update_finger_table(self.predecessor.node_id, self.predecessor.host, self.predecessor.port, True)
                                self.successor = self.predecessor
                                # inform successor to update pred_pointed_table
                                self.send_message(self.successor.socket, {"method": "updatePPT", "PPT": {node_id: row[:-1] for node_id, row in self.pointed_table.items()}})
                                self.send_message(self.successor.socket, {"method": "imYourPred", "host": self.host, "port": self.port, "node_id": self.node_id})
                            else:   # new node receives
                                self.predecessor = Node(pred_host, pred_port, request.get("node_id"), sock)
                                # update finger table to include predecessor
                                self.update_finger_table(self.predecessor.node_id, self.predecessor.host, self.predecessor.port, False)
                                self._establish_chord()
                        else:   # ring member receives
                            # inform predecessor
                            self.send_message(self.predecessor.socket, {"method": "yourNewSucc", "host": pred_host, "port": pred_port, "node_id": request.get("node_id")})
                            self.last_pred_id = self.predecessor.node_id
                            # set new predecessor
                            self.predecessor = Node(pred_host, pred_port, request.get("node_id"), sock)
                            # update finger table to include predecessor
                            self.update_finger_table(self.predecessor.node_id, self.predecessor.host, self.predecessor.port, True)
                            # notify all its PT of new pred's existance
                            for node_id, row in self.pointed_table.items():
                                self.send_message(row[-1], {"method": "newNode", "node_id": self.predecessor.node_id, "host": self.predecessor.host, "port": self.predecessor.port})

                    elif method == "readyForDataTransfer":
                        # transfer original predecessor's replication data to new predecessor, and delete them
                        to_delete = set()
                        for key, val in self.spreadsheet.data.items():
                            if int(key) <= self.last_pred_id:
                                self.send_message(self.predecessor.socket, {"method": "insert_replication", "repli_key": key, "value": val})
                                to_delete.add(key)
                        for key in to_delete:
                            self.spreadsheet.remove(key)
                        # transfer new node's responsible data to it
                        for key, val in self.spreadsheet.data.items():
                            if not self._isResponsible(key):
                                self.send_message(self.predecessor.socket, {"method": "insert_replication", "repli_key": key, "value": val})
                                self.send_message(self.successor.socket, {"method": "remove_replication", "repli_key": key})

                    elif method == "yourNewSucc":
                        succ_host, succ_port = request.get("host"), request.get("port")
                        self.successor = Node(succ_host, succ_port, request.get("node_id"))
                        self.send_message(self.successor.socket, {"method": "updatePPT", "PPT": {node_id: row[:-1] for node_id, row in self.pointed_table.items()}})
                        # update finger table to include successor
                        self.update_finger_table(self.successor.node_id, self.successor.host, self.successor.port, True)
                        self.send_message(self.successor.socket, {"method": "imYourPred", "host": self.host, "port": self.port, "node_id": self.node_id})
                    
                    elif method == "establishChord":
                        message = {"status": "success", "host": f"{self.host}", "port": f"{self.port}", "node_id": f"{self.node_id}"}
                        if request.get("msg_id"):
                            message["msg_id"] = request.get("msg_id")
                        self.send_message(sock, message)

                    elif method == "imPointingAtYou":
                        if request.get("node_id") in self.pointed_table:
                            self.pointed_table[request.get("node_id")][0] += 1
                        else:
                            self.pointed_table[request.get("node_id")] = [1, request.get("host"), request.get("port"), sock]

                    elif method == "imNotPointingAtYou":
                        self.pointed_table[request.get("node_id")][0] -= 1
                        if self.pointed_table[request.get("node_id")][0] == 0:
                            del self.pointed_table[request.get("node_id")]

                    elif method == "chordEstablishmentCompleted":
                        # inform successor to update pred_pointed_table
                        if self.successor:
                            self.send_message(self.successor.socket, {"method": "updatePPT", "PPT": {node_id: row[:-1] for node_id, row in self.pointed_table.items()}})
                            
                    elif method == "newNode":
                        self.update_finger_table(request.get("node_id"), request.get("host"), request.get("port"), True)

                    elif method == "updatePFT":
                        self.pred_finger_table = request.get("PFT")
                    
                    elif method == "updatePPT":
                        self.pred_pointed_table = request.get("PPT")

                    elif method == "askForFT":
                        return {"FT": [row[:-1] for row in self.finger_table]}
                    else:
                        pass
                else:   # not responsible, route to target "key"
                    if "msg_id" not in request: # client reach out to chord, add msg_id to the request
                        request["msg_id"] = f"{self.node_id}_{self.msg_counter}"
                        self.msg_counter += 1
                    
                    next_socket = self._route(request["key"], request)  # route to key
                    self.message_dic[request["msg_id"]] = (sock, next_socket)

        except Exception as e:
            print("error in handling request")
            print(e)
            # return {"status": "error", "message": f"Invalid request {request}; method required"}
    
    def _inInterval(self, start, end, val):
        """ test if val is in [start, end) in the chord """
        if start <= end:
            return start <= val < end
        return not (end <= val < start)

    def _isResponsible(self, key):
        """ test if is responsible for this key (lookup) """
        if not self.predecessor: return True
        key = int(key)
        return self._inInterval(self.predecessor.node_id+1, self.node_id+1, key)

    def _route(self, target_id, message):
        """ route target_id based on finger table """
        for i in range(FINGER_NUM):
            last = self.finger_table[i-1][0]
            curr = self.finger_table[i][0]
            if self._inInterval(last, curr, target_id):
                print(f"routing to {self.finger_table[i-1][1]}")
                if self.finger_table[i-1][-1]:
                    self.send_message(self.finger_table[i-1][-1], message)
                    return self.finger_table[i-1][-1]
        # no match => only two nodes, so route to the other node
        self.send_message(self.finger_table[0][-1], message)
        return self.finger_table[0][-1]

    def update_finger_table(self, joining_node_id, joining_host, joining_port, updateTargetPT):
        """ Update the finger table entries when a new node joins. """
        affected_sockets = set()
        for i in range(FINGER_NUM):
            if self._inInterval(self.finger_table[i][0], self.finger_table[i][1], joining_node_id) and joining_node_id != self.finger_table[i][1]:
                if self.finger_table[i][-1]:
                    self.send_message(self.finger_table[i][-1], {"method": "imNotPointingAtYou", "node_id": self.node_id})
                    affected_sockets.add(self.finger_table[i][-1])
                self.finger_table[i][1:] = joining_node_id, joining_host, joining_port, None
                if self.predecessor and joining_node_id == self.predecessor.node_id:
                    self.finger_table[i][-1] = self.predecessor.socket
                if self.successor and joining_node_id == self.successor.node_id:
                    self.finger_table[i][-1] = self.successor.socket
                if self.finger_table[i][-1] is None:
                    self.finger_table[i][-1] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.finger_table[i][-1].connect((joining_host, joining_port))
                if updateTargetPT:  # inform target to update pointed_table
                    self.send_message(self.finger_table[i][-1], {"method": "imPointingAtYou", "node_id": self.node_id, "host": self.host, "port": self.port})
                    affected_sockets.add(self.finger_table[i][-1])
        # inform successor to update pred_finger_table
        if self.successor and self.successor.socket:
            self.send_message(self.successor.socket, {"method": "updatePFT", "PFT": [row[:-1] for row in self.finger_table]})
        for sock in affected_sockets:
            self.send_message(sock, {"method": "chordEstablishmentCompleted"})


def start_server(project_name, node_id):
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.bind(('', 0))
        master_socket.listen(5)
        server = SpreadSheetServer(project_name, node_id, socket.getfqdn(), master_socket.getsockname()[1])
        print(f"Listening on port {server.port}")
        if server.successor:
            print(server.successor.node_id)
        # Background threads
        threading.Thread(target=register_name_server, args=(server.port, f'{project_name}_{node_id}'), daemon=True).start()
        threading.Thread(target=print_info, args=(server,), daemon=True).start()

        server.client_sockets = {}
        while True:
            # check successor
            if server.successor and server.successor.socket and server.successor.socket not in server.client_sockets:
                if server.successor.socket.fileno() == -1:
                    print("Successor socket is invalid or closed")
                    server.successor.socket = None
                else:
                    server.client_sockets[server.successor.socket] = (server.successor.host, server.successor.port)
            # check predecessor
            if server.predecessor and server.predecessor.socket and server.predecessor.socket not in server.client_sockets:
                if server.predecessor.socket.fileno() == -1:
                    print("Predecessor socket is invalid or closed")
                    server.predecessor.socket = None
                    server.handle_pred_failure()
                else:
                    server.client_sockets[server.predecessor.socket] = (server.predecessor.host, server.predecessor.port)
            # check finger table
            for i in range(FINGER_NUM):
                target_id, node_id, host, port, sock = server.finger_table[i]
                if node_id == server.node_id:   # pointing to itself => ignore
                    continue
                if sock is None:    # row removed => ignore
                    continue
                if sock.fileno() == -1: # socket invalid => reset row in finger table
                    print(f"Fingertable socket ({node_id}) is invalid or closed")
                    server.finger_table[i][2:] = None, None, None
                    continue
                if sock not in server.client_sockets:   # new socket => add to client_sockets
                    server.client_sockets[sock] = (host, port)

            sockets_to_read = [master_socket] + list(server.client_sockets.keys())

            readable_sockets, _, _ = select.select(sockets_to_read, [], [])

            for sock in readable_sockets:
                if not sock:
                    print("skipping connection")
                    continue
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
                        del server.client_sockets[sock]
                        # print(f"{server.client_sockets[sock]} disconnected unexpectedly: {e}")
                    except json.JSONDecodeError:
                        print(f"Received malformed JSON from {server.client_sockets[sock]}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 SpreadSheetServer.py <project_name> <node_id>")
        sys.exit(1)
    start_server(sys.argv[1], sys.argv[2])