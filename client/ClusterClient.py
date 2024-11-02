import socket
import json
import time
from hashlib import sha256
from SpreadSheetClient import SpreadSheetClient

class ClusterClient:
    def __init__(self, project_name, N, K):
        """
        Initialize the ClusterClient
        :param project_name: The base name of the server cluster
        :param N: Number of servers
        :param K: Number of replicas (how many servers each value will be stored on)
        """
        self.project_name = project_name
        self.N = N      # server num
        self.K = K      # replica num
        self.servers = [SpreadSheetClient(f"{project_name}-{i}") for i in range(N)]

    def _get_server_indices(self, row, col):
        S = int(sha256(f"{row},{col}".encode()).hexdigest(), 16) % self.N
        return [(S + i) % self.N for i in range(self.K)]

    def insert(self, row, col, value):
        server_indices = self._get_server_indices(row, col)
        res = None
        for server_index in server_indices:
            while True:
                try:
                    res = self.servers[server_index].insert(row, col, value)
                    if res: break
                except Exception as e:
                    print(f"Error inserting on server {server_index}: {e}, retrying...")
                time.sleep(5)
        return res

    def lookup(self, row, col):
        server_indices = self._get_server_indices(row, col)
        res = None
        while not res:
            for server_index in server_indices:
                try:
                    res = self.servers[server_index].lookup(row, col)
                    if res: return res
                except Exception as e:
                    print(f"Error looking up on server {server_index}: {e}, trying next replica...")
            print(f"Value not found at ({row}, {col}) on any server")
            time.sleep(5)

    def remove(self, row, col):
        server_indices = self._get_server_indices(row, col)
        res = None
        for server_index in server_indices:
            while True:
                try:
                    res = self.servers[server_index].remove(row, col)
                    if res: break
                except Exception as e:
                    print(f"Error removing on server {server_index}: {e}, retrying...")
                time.sleep(5)
        return res

    def size(self):
        res_col, res_row = -1, -1
        for server_index in range(self.N):
            while True:
                try:
                    result = self.servers[server_index].size()
                    if result and result['status'] == 'success':
                        res_col = max(res_col, int(result['max_col']))
                        res_row = max(res_row, int(result['max_row']))
                    if result: break
                except Exception as e:
                    print(f"Error getting size of server {server_index}: {e}")
                time.sleep(5)
        if res_col == -1 or res_row == -1:
            return {'status': 'failure', 'message': 'SpreadSheet empty'}
        return {'status': 'success', 'max_row': res_row, 'max_col': res_col}

    def query(self, row, col, width, height):
        res = {}
        for server_index in range(self.N):
            while True:
                try:
                    result = self.servers[server_index].query(row, col, width, height)
                    res |= result
                    if result: break
                except Exception as e:
                    print(f"Error querying server {server_index}: {e}")
                time.sleep(5)
        return res
