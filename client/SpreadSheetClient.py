# SpreadSheetClient

import socket
import json
import requests
import time
import random

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

            # select a random server
            # retry connecting to service (loop through all possible names)
            services = [service for service in services if service.get("type") == "spreadsheet" and service.get("project").split('_')[0] == self.project_name.split('_')[0]]
            random.shuffle(services)
            for service in services:
                try:
                    self.host = service.get("name")
                    self.port = service.get("port")
                    self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.client_socket.connect((self.host, self.port)) # connect to this server, and send join request
                    print(f'connecting to: {self.host, self.port}')
                    break
                except:
                    pass
            

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

    def insert(self, key, value):
        request = {"method": "insert", "key": key, "value": value}
        return self.send_request(request)

    def lookup(self, key):
        request = {"method": "lookup", "key": key}
        return self.send_request(request)

    def remove(self, key):
        request = {"method": "remove", "key": key}
        return self.send_request(request)

