# TestInsert

import sys
import time
from SpreadSheetClient import SpreadSheetClient
import os
import random

FINGER_NUM  = 16
MAX_KEY     = 2 ** FINGER_NUM
ITERATIONS  = 1000

def measure(client, operation, *args):
    start = time.time()
    result = operation(*args)
    end = time.time()
    return result, end - start

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 TestInsert.py <project_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    client = SpreadSheetClient(project_name)
    pid = os.getpid()
    
    testList = random.sample(range(MAX_KEY), ITERATIONS)

    # insert
    total_insert_time = 0
    for i in testList:
        result, duration = measure(client, client.insert, i, {"value": i})
        total_insert_time += duration
    # print("insert complete")

    print(f"insert\tthroughput: {ITERATIONS / total_insert_time:4.6f}\tops/sec\tlatency: {total_insert_time / ITERATIONS:.6f} sec")