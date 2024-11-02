# SpreadSheetServer

import socket
import json
import sys
import time
import threading
import os
from SpreadSheet import SpreadSheet
import select

def register_name_server(port, project_name):
    name_server_address = ("catalog.cse.nd.edu", 9097)
    while True:
        message = {
            "type": "spreadsheet",
            "owner": "lli27",
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

def handle_request(spreadsheet, request):
    try:
        method = request.get("method")
        if method == "insert":
            return spreadsheet.insert(request["row"], request["column"], request["value"])
        elif method == "lookup":
            return spreadsheet.lookup(request["row"], request["column"])
        elif method == "remove":
            return spreadsheet.remove(request["row"], request["column"])
        elif method == "size":
            return spreadsheet.size()
        elif method == "query":
            return spreadsheet.query(request["row"], request["column"], request["width"], request["height"])
        else:   # the entered method not found
            return {"status": "error", "message": "Invalid method. (insert/lookup/remove/size/query)"}
    except: # other problems (i.e. key method not found)
        return {"status": "error", "message": "Invalid request; method required"}


def start_server(project_name):
    spreadsheet = SpreadSheet()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
        master_socket.bind(('', 0))
        master_socket.listen(5)
        port = master_socket.getsockname()[1]
        print(f"Listening on port {port}")

        # background thread to register name server
        threading.Thread(target=register_name_server, args=(port, project_name), daemon=True).start()

        client_sockets = {}
        while True:
            sockets_to_read = [master_socket] + list(client_sockets.keys())
            readable_sockets, _, _ = select.select(sockets_to_read, [], [])

            for sock in readable_sockets:
                if sock is master_socket:   # new connection
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
                        response = handle_request(spreadsheet, request)
                        response_data = f'{json.dumps(response)}\n'.encode('utf-8')
                        sock.sendall(response_data)    # send response

                    except EOFError:    # client disconnected
                        print(f"Client {sock.getpeername()} disconnected")
                        sock.close()
                        del client_sockets[sock]
                        print(client_sockets)
                    except (ConnectionResetError, BrokenPipeError) as e:
                        print(f"Client {addr} disconnected unexpectedly: {e}")
                    except json.JSONDecodeError:
                        print(f"Received malformed JSON from {addr}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 SpreadSheetServer.py <project_name>")
        sys.exit(1)
    start_server(sys.argv[1])
