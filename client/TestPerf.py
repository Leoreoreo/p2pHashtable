# TestPerf

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
        print("Usage: python3 TestPerf.py <project_name>")
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
    
    # lookup  
    total_lookup_time = 0
    for i in testList:
        result, duration = measure(client, client.lookup, i)
        total_lookup_time += duration
    # print("lookup complete")

    # remove  
    total_remove_time = 0
    for i in testList:
        result, duration = measure(client, client.remove, i)
        total_remove_time += duration
    # print("remove complete")

    print()
    print(f"insert\tthroughput: {ITERATIONS / total_insert_time:4.6f}\tops/sec\tlatency: {total_insert_time / ITERATIONS:.6f} sec")
    print(f"lookup\tthroughput: {ITERATIONS / total_lookup_time:4.6f}\tops/sec\tlatency: {total_lookup_time / ITERATIONS:.6f} sec")
    print(f"remove\tthroughput: {ITERATIONS / total_remove_time:4.6f}\tops/sec\tlatency: {total_remove_time / ITERATIONS:.6f} sec")