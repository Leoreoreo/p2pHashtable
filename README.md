# p2pHashtable
- Final Project for FA24-CSE-40771 Distributed Systems
  - @ University of Notre Dame
 
## Authors
- Leyang Li (lli27@nd.edu)
- Zanxiang Yin (zyin5@nd.edu)

## Usage
### Environment
- python stardard library
```
Python 3.10.14 | packaged by conda-forge | (main, Mar 20 2024, 12:45:18) [GCC 12.3.0] on linux
```
- using Notre Dame name server:
  - http://catalog.cse.nd.edu:9097/
  - remember to change it in `server/SpreadSheetServer.py` and `client/SpreadSheetClient.py`
 
### Parameters
- Fingertable size: `16`
- Max key: `2**16 = 65536`
  - can change `FINGER_NUM` in `server/SpreadSheetServer.py` and `client/TestPerf.py`

### Run Server(s)
run the following command `python3 ./server/SpreadSheetServer.py <project_name> <node_id>`
- at least three times (more is better), each in one terminal
- with the same `project_name` (string)
- with different `node_id` (int)


### Run Tests
#### Test Basic Functions
```
python3 ./client/TestBasics.py <project_name>
```
#### Test Throughputs
```
python3 ./client/TestPerf.py <project_name>
```

## Documents
- [project proposal](https://docs.google.com/document/d/1WbyIjw985jdG8tDCrGutfF6qgVYsxmeQ8zx6wO8MM0A/edit?tab=t.0)
- [project report](https://docs.google.com/document/d/1BwiXdTeq11H4dstQn3BZIUQmUbn_gdoFPMAWAzMkHok/edit?tab=t.0)
- [presentation](https://docs.google.com/presentation/d/1gtjw9OqKUPV1NYyNfGNaOPeqVxXBwR-J/edit#slide=id.g31b1776cccc_0_69)
