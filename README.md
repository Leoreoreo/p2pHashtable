# p2pHashtable
- Final Project for FA24-CSE-40771 Distributed Systems
  - @ University of Notre Dame
 
## Authors
- Leyang Li (lli27@nd.edu)
- Zanxiang Yin (zyin5@nd.edu)

## Project Description
- [Description PDF](https://github.com/Leoreoreo/p2pHashtable/blob/main/Peer-to-Peer%20Hash%20Table.pdf)


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
  - can change `FINGER_NUM` in `server/SpreadSheetServer.py` and `client/Test*.py`

### Run Server(s)
run the following command `python3 ./server/SpreadSheetServer.py <project_name> <node_id>`
- at least three times (more is better), each in one terminal
- with the same `project_name` (string)
- with different `node_id` (int)


### Run Tests
#### Test Basic Functions
you can edit the `TestBasics.py` to test the client operations
```
python3 ./client/TestBasics.py <project_name>
```
#### Test Throughputs
Test the throughput for each operation:
- insert (insert 1000 random keys)
```
python3 ./client/TestInsert.py <project_name>
```
- remove (remove 1000 random keys)
```
python3 ./client/TestRemove.py <project_name>
```
- lookup (lookup 1000 random keys)
```
python3 ./client/TestLookUp.py <project_name>
```
- [test results](https://colab.research.google.com/drive/1Kl1z5VYx7zStE08ROs4KYZK5JeeazpN_?usp=sharing)

## Documents (Require access)
- [project proposal](https://docs.google.com/document/d/1WbyIjw985jdG8tDCrGutfF6qgVYsxmeQ8zx6wO8MM0A/edit?tab=t.0)
- [project report](https://docs.google.com/document/d/1BwiXdTeq11H4dstQn3BZIUQmUbn_gdoFPMAWAzMkHok/edit?tab=t.0)
- [presentation](https://docs.google.com/presentation/d/1gtjw9OqKUPV1NYyNfGNaOPeqVxXBwR-J/edit#slide=id.g31b1776cccc_0_69)
- [final report](https://docs.google.com/document/d/1GW5MUFwgestbj05B2qW5W1NxJ_zdWABMkM4yA-rOzL0/edit?tab=t.0#heading=h.sia5i7iltmnd)
