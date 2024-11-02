# SpreadSheetClient

import socket
import json
import requests
import time

class SpreadSheetClient:
    def __init__(self, project_name):
        self.host = None
        self.port = None
        self.project_name = project_name
        self.client_socket = None
        self._re_connect()  # set host and port
        
    def _re_connect(self):
        if self.client_socket:
            self.client_socket.close()
        try:
            response = requests.get("http://catalog.cse.nd.edu:9097/query.json")    # name server
            services = response.json()
            service = max([service for service in services if service.get("type") == "spreadsheet" and service.get("project") == self.project_name], key=lambda x: x.get("lastheardfrom"))
            self.host = service.get("name")
            self.port = service.get("port")
            print(f'connecting to: {self.host, self.port}')

            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.host, self.port))
        except:
            print('project not found')

    def send_request(self, request):
        try:
            request_data = f'{json.dumps(request)}\n'.encode('utf-8')
            self.client_socket.sendall(request_data)
            self.client_socket.settimeout(5)             # wait at most 5 sec

            response_data = b''
            while not response_data.endswith(b'\n'):
                more = self.client_socket.recv(1)
                if not more:
                    raise EOFError("Socket connection broken")
                response_data += more
            return json.loads(response_data.decode('utf-8').strip())
        except Exception as e:
            print(f"Request: {request}\n Error: {e}\n")

    def insert(self, row, col, value):
        request = {"method": "insert", "row": row, "column": col, "value": value}
        return self.send_request(request)

    def lookup(self, row, col):
        request = {"method": "lookup", "row": row, "column": col}
        return self.send_request(request)

    def remove(self, row, col):
        request = {"method": "remove", "row": row, "column": col}
        return self.send_request(request)

    def size(self):
        request = {"method": "size"}
        return self.send_request(request)

    def query(self, row, col, width, height):
        request = {"method": "query", "row": row, "column": col, "width": width, "height": height}
        return self.send_request(request)
