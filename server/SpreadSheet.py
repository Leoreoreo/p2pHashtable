# SpreadSheet

import os, json

class SpreadSheet:
    def __init__(self, node_id, log_max_size=100):
        self.data = {}
        self.node_id = node_id
        # self.ckpt_path = f"ckpt/{str(node_id)}/sheet.ckpt"
        # self.log_path = f"log/{str(node_id)}/sheet.log"
        # self.log_max_size = log_max_size
        # self.log_size = 0
        # # Ensure directories exist
        # os.makedirs(os.path.dirname(self.ckpt_path), exist_ok=True)
        # os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

        # # Empty the log and checkpoint files
        # open(self.ckpt_path, "w").close()  # Truncate or create ckpt file
        # open(self.log_path, "w").close()  # Truncate or create log file
        
        self._recover()
        # self.log = open(self.log_path, "a")
        

    # recover from crash: load checkpoint and then replay log
    def _recover(self):
        self.data = {}
        # # load from checkpoint
        # try:
        #     with open(self.ckpt_path, "r") as ckpt:
        #         self.data = json.load(ckpt)
        # except Exception as e:
        #     self.data = {}
        
        # # replay log
        # try:
        #     with open(self.log_path, "r") as log:
        #         for line in log:
        #             self.log_size += 1
        #             log_dic = json.loads(line)
        #             method = log_dic["method"]
        #             key = log_dic["key"]
        #             value = log_dic["value"]
        #             if method == "insert":
        #                 self.data[f'{key}'] = value
        #             elif method == "remove" and f'{key}' in self.data:
        #                 del self.data[f'{key}']
        # except Exception as e:
        #     pass
        
        print("recover complete")
        print(self.data)

    def _compact_log(self):
        # Ensure the checkpoint directory exists
        os.makedirs(os.path.dirname(self.ckpt_path), exist_ok=True)
        
        # Save RAM data into ckpt_new file
        try:
            with open(self.ckpt_path + '_new', "w") as newckpt:
                json.dump(self.data, newckpt)
                newckpt.flush()
                os.fsync(newckpt.fileno())
        except Exception as e:
            print(f"Error writing checkpoint: {e}")
            return
        
        # Replace the old ckpt: by renaming ckpt_new into ckpt
        try:
            os.rename(self.ckpt_path + '_new', self.ckpt_path)
        except Exception as e:
            print(f"Error renaming checkpoint: {e}")
            return
        
        # Clear the log: by closing log, open it in write mode, and re-open it for appending
        self.log.close()
        with open(self.log_path, 'w') as f:
            pass
        self.log = open(self.log_path, "a")
        self.log_size = 0
    
    # append to log file, check if it requires compacting
    def _write_log(self, method, key, value=None):
        self.log.write(json.dumps({"method": method, "key": key, "value": value}) + "\n")
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


    def insert(self, key, value):
        # check input
        try:
            key = int(key)
        except:
            return {"status": "failure", "message": "Invalid key value"}
        if not self._are_positive_int([key]):
            return {"status": "failure", "message": "Invalid key value"}
        # insert
        self.data[f'{key}'] = value
        # # append to log
        # self._write_log("insert", key, value)
        # return
        return {"status": "success"}

    def lookup(self, key):
        # check input
        try:
            key = int(key)
        except:
            return {"status": "failure", "message": "Invalid key value"}
        if not self._are_positive_int([key]):
            return {"status": "failure", "message": "Invalid key value"}
        # lookup
        if f'{key}' in self.data:
            return {
                "status": "success", 
                "value": self.data[f'{key}']
            }
        # not found
        return {
            "status": "failure", 
            "message": "Key not found"
        }

    def remove(self, key):
        # check input
        try:
            key = int(key)
        except:
            return {"status": "failure", "message": "Invalid key value"}
        if not self._are_positive_int([key]):
            return {"status": "failure", "message": "Invalid key value"}

        # remove
        if f'{key}' in self.data:
            del self.data[f'{key}']
            # append to log
            # self._write_log("remove", key)
            return {"status": "success"}

        # not found
        return {
            "status": "failure", 
            "message": "Key not found"
        }

