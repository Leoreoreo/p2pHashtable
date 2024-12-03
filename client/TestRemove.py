# TestRemove

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
        print("Usage: python3 TestRemove.py <project_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    client = SpreadSheetClient(project_name)
    pid = os.getpid()
    
    testList = random.sample(range(MAX_KEY), ITERATIONS)

    # remove  
    total_remove_time = 0
    for i in testList:
        result, duration = measure(client, client.remove, i)
        total_remove_time += duration
    # print("remove complete")

    print(f"remove\tthroughput: {ITERATIONS / total_remove_time:4.6f}\tops/sec\tlatency: {total_remove_time / ITERATIONS:.6f} sec")