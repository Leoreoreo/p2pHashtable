# TestLookUp

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
        print("Usage: python3 TestLookUp.py <project_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    client = SpreadSheetClient(project_name)
    pid = os.getpid()
    
    testList = random.sample(range(MAX_KEY), ITERATIONS)

    
    # lookup  
    total_lookup_time = 0
    for i in testList:
        result, duration = measure(client, client.lookup, i)
        total_lookup_time += duration
    # print("lookup complete")

    print(f"lookup\tthroughput: {ITERATIONS / total_lookup_time:4.6f}\tops/sec\tlatency: {total_lookup_time / ITERATIONS:.6f} sec")