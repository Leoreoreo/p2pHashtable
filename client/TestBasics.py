# TestBasics

import sys
from SpreadSheetClient import SpreadSheetClient

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 TestBasics.py <project_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    client = SpreadSheetClient(project_name)

    print(client.insert(2, {"weight": 100, "animal": "zebra"}))
    print(client.insert(20000, {"weight": 100, "animal": "zebra"}))
    print(client.insert(40000, {"weight": 100, "animal": "zebra"}))
    # print(client.lookup(2))

    # print(client.remove(2))
    # print(client.lookup(2))

    # print(client.invalid_req("afgrewa"))