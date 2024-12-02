# p2pHashtable
- Final Project for FA24-CSE-40771 Distributed Systems
 
## People
- Zanxiang Yin (zyin5@nd.edu)
- Leyang Li (lli27@nd.edu)


## Usage
### Description
- Fingertable size: `16`
- Max key: `2**16 = 65536`

### Run Server(s)
run the following command 
- at least three times, each in one terminal
- with the same `project_name`
- with different `node_id`
```
python3 ./server/SpreadSheetServer.py <project_name> <node_id>
```

### Run TestBasics
```
python3 ./client/TestBasics.py <project_name>
```
- expected output:
```
connecting to: (...(host), ...(port))
{'status': 'success', ...}
{'status': 'success', 'value': {'weight': 100, 'animal': 'zebra'}, ...}
{'status': 'failure', 'message': 'Key not found', ...}
{'status': 'success', ...}
{'status': 'failure', 'message': 'Key not found', ...}
```

## Documents
- [project proposal](https://docs.google.com/document/d/1WbyIjw985jdG8tDCrGutfF6qgVYsxmeQ8zx6wO8MM0A/edit?tab=t.0)
- [project report](https://docs.google.com/document/d/1BwiXdTeq11H4dstQn3BZIUQmUbn_gdoFPMAWAzMkHok/edit?tab=t.0)
- [presentation](https://docs.google.com/presentation/d/1gtjw9OqKUPV1NYyNfGNaOPeqVxXBwR-J/edit#slide=id.g31b1776cccc_0_69)
