# SpreadSheet

import os, json

class SpreadSheet:
    def __init__(self, node_id, ckpt_path="_sheet.ckpt", log_path="_sheet.log", log_max_size=100):
        self.data = {}
        self.node_id = node_id
        self.ckpt_path = str(node_id) + ckpt_path
        self.log_path = str(node_id) + log_path
        self.log_max_size = log_max_size
        self.log_size = 0
        self._recover()
        self.log = open(log_path, "a")
        

    # recover from crash: load checkpoint and then replay log
    def _recover(self):
        # load from checkpoint
        try:
            with open(self.ckpt_path, "r") as ckpt:
                self.data = json.load(ckpt)
        except Exception as e:
            self.data = {}
        
        # replay log
        try:
            with open(self.log_path, "r") as log:
                for line in log:
                    self.log_size += 1
                    log_dic = json.loads(line)
                    method = log_dic["method"]
                    row = log_dic["row"]
                    col = log_dic["col"]
                    value = log_dic["value"]
                    if method == "insert":
                        self.data[f'{row} {col}'] = value
                    elif method == "remove" and f'{row} {col}' in self.data:
                        del self.data[f'{row} {col}']
        except Exception as e:
            pass
        
        print("recover complete")
        print(self.data)

    # save RAM data into ckpt, clear the log
    def _compact_log(self):
        # save RAM data into ckpt_new file
        try:
            with open(self.ckpt_path+'_new', "w") as newckpt:
                json.dump(self.data, newckpt)
                newckpt.flush()
                os.fsync(newckpt.fileno())
        except Exception as e:
            print(f"Error writing checkpoint: {e}")
            return
        # replace the old ckpt: by renaming ckpt_new into ckpt
        os.rename(self.ckpt_path+'_new', self.ckpt_path)
        # clear the log: by closing log, open it in w and close it, and then re-open it
        self.log.close()
        with open(self.log_path, 'w') as f:
            pass
        self.log = open(self.log_path, "a")
        self.log_size = 0
    
    # append to log file, check if it requires compacting
    def _write_log(self, method, row, col, value=None):
        self.log.write(json.dumps({"method": method, "row": row, "col": col,"value": value}) + "\n")
        self.log.flush()
        os.fsync(self.log.fileno())
        self.log_size += 1
        if self.log_size >= self.log_max_size:
            self._compact_log()

    # check input: everything in lst are int
    def _are_positive_int(self, lst):
        for val in lst:
            if type(val) != int or val < 0:
                return False
        return True


    def insert(self, row, col, value):
        # check input
        try:
            row, col = int(row), int(col)
        except:
            return {"status": "failure", "message": "Invalid row/column value"}
        if not self._are_positive_int([row, col]):
            return {"status": "failure", "message": "Invalid row/column value"}
        # insert
        self.data[f'{row} {col}'] = value
        # append to log
        self._write_log("insert", row, col, value)
        # return
        return {"status": "success"}

    def lookup(self, row, col):
        # check input
        try:
            row, col = int(row), int(col)
        except:
            return {"status": "failure", "message": "Invalid row/column value"}
        if not self._are_positive_int([row, col]):
            return {"status": "failure", "message": "Invalid row/column value"}
        # lookup
        if f'{row} {col}' in self.data:
            return {
                "status": "success", 
                "value": self.data[f'{row} {col}']
            }
        # not found
        return {
            "status": "failure", 
            "message": "Cell not found"
        }

    def remove(self, row, col):
        # check input
        try:
            row, col = int(row), int(col)
        except:
            return {"status": "failure", "message": "Invalid row/column value"}
        if not self._are_positive_int([row, col]):
            return {"status": "failure", "message": "Invalid row/column value"}

        # remove
        if f'{row} {col}' in self.data:
            del self.data[f'{row} {col}']
            # append to log
            self._write_log("remove", row, col)
            return {"status": "success"}

        # not found
        return {
            "status": "failure", 
            "message": "Cell not found"
        }

