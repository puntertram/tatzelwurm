import fcntl
import os 
import time
import threading

class LockObject:
    def __init__(self, namespace, config):
        self.config = config
        self.namespace = namespace
    
    def grab_lock(self):
        try:
            self.fd = os.open(os.path.join(self.config["PERSISTENT_HASHMAP_WAL_DIRECTORY"], f"{self.namespace}_WAL.lock"), os.O_WRONLY)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as e:
            print(f"{threading.get_ident()}, {self.namespace} Cannot secure lock on PERSISTENT_HASHMAP_WAL_DIRECTORY", e)
            return -1
        else:
            print(f"{threading.get_ident()}, {self.namespace} Secured the lock, continuing...")
            return self.fd

    def remove_lock(self):
        # Remove the lock on ARCHIVE_DIR
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        print(f"{threading.get_ident()}, Removed the lock on {self.namespace}")


class PersistentHashMap:
    def __init__(self, namespace, audit_log_file_path, config) -> None:
        self.audit_log_file_path = audit_log_file_path
        self.namespace = namespace
        self.config = config 
        self.lock_object = LockObject(self.namespace, self.config)
        try:
            self.audit_file = open(self.audit_log_file_path, "x+")
            print(f"[PersistentHashMap {self.namespace}] did not see any WAL log, so this is a fresh instance...")
        except FileExistsError:
            self.audit_file = open(self.audit_log_file_path, "a+")
            print(f"[PersistentHashMap {self.namespace}] Recovering the hashmap from the WAL log")
        self.init()
    
    def init(self):
        self.dict = {}
        self.audit_file.seek(0)
        lines = self.audit_file.readlines()
        print(lines)
        # if len(lines) == 0:
        #     print(f"[PersistentHashMap {self.namespace}] did not see any WAL log, so this is a fresh instance...")
        # else:
        #     print("[PersistentHashMap {self.namespace}] Recovering the hashmap from the WAL log")
        for line in lines:
            request_type, *remaining = line.split("|")
            if request_type == "add":
                key, value = remaining
                value = value.replace("\n", "")
                self.dict[key] = value 
            elif request_type == "remove":
                key = remaining[0]
                del self.dict[key]
            elif request_type == "update":
                key, value = remaining
                self.dict[key] = value 

    def put(self, key, value):
        while True:
            time.sleep(1)
            print(f"{threading.get_ident()}, Trying to secure lock... {self.namespace}")
            if self.lock_object.grab_lock() != -1:
                break
        self.audit_file.write(f"add|{key}|{value}\n")
        self.dict[key] = value
        self.audit_file.flush()
        self.lock_object.remove_lock()
        
    
    def get(self, key):
        return self.dict.get(key)

            