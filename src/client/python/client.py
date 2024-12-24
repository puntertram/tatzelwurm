import requests
import sys 
import json
from urllib.parse import urljoin
import os
from functools import reduce
CLIENT_CONFIG_FILE_PATH = sys.argv[1]
client_config = json.load(open(CLIENT_CONFIG_FILE_PATH))

def connect():
    # get the chunk size
    # setup a connection with the master server
    assert requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "heartbeat")).status_code == 200, "Connection to the GFS server failed..."
    print("Connection to master succeeded")
    chunk_size_request_json = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "heartbeat")).json()
    client_config["gfs_chunk_size"] = int(chunk_size_request_json["chunk_size"]) 

def chunk_server_is_live(chunk_server):
    print(chunk_server)
    if requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat")).status_code == 200:
        return True 
    else:
        return False

def read_file(file_name, position, size):
    next_chunk_position = ((size // client_config["gfs_chunk_size"]) + 1) * client_config["gfs_chunk_size"]
    previous_chunk_position = (position // client_config["gfs_chunk_size"]) * client_config["gfs_chunk_size"]
    number_of_requests = (next_chunk_position - previous_chunk_position) // client_config["gfs_chunk_size"]
    data = ["*"] * number_of_requests
    print("The number of requests is", number_of_requests)
    for request_id in range(number_of_requests):
        # from the chunk size convert to a chunk offset in the file
        chunk_offset = int(position // client_config["gfs_chunk_size"]) + request_id
        # get the chunk id, and the list of chunkservers to reach out to from the master
        get_chunk_id_request = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_id"), params={"file_name": file_name, "chunk_offset": chunk_offset})
        if get_chunk_id_request.status_code == 404:
            print("The requested chunk_id does not exist!")
        elif get_chunk_id_request.status_code == 409:
            chunk_id = get_chunk_id_request.json()["chunk_id"]       
            chunk_servers = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"), params={"chunk_id": chunk_id}).json()["chunk_servers"]               
            print(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"))
            print({"chunk_id": chunk_id})
            # connect to the chunkservers in a fault tolerant way and get the chunk contents
            connected_with_chunk_server = False 
            active_chunk_server = None 
            number_of_retries = 0
            get_chunk_is_successful = False
            while number_of_retries < client_config["GET_CHUNK_NUMBER_OF_RETRIES"]:
                print(chunk_servers)
                for chunk_server_idx, chunk_server in enumerate(chunk_servers):
                    if not chunk_server.get("marked", False) and chunk_server_is_live(chunk_server):
                        chunk_server_heartbeat_request = requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat"))
                        if chunk_server_heartbeat_request.status_code == 200:
                            active_chunk_server = chunk_server
                        connected_with_chunk_server = True
                        chunk_server["marked"] = True 
                        break 
                if connected_with_chunk_server == False:
                    # Return an error and fail this request
                    print("""Could not connect with any of the chunk_servers...\nHere is a list of the chunk_servers...""")
                    print(chunk_servers)
                else:
                    chunk_request = requests.get(urljoin(active_chunk_server["ipv4_address"], "get_chunk"), params={"chunk_id": chunk_id})
                    if chunk_request.status_code == 200:
                        print(f"[{chunk_id}]get_chunk request is successful")
                        get_chunk_is_successful = True 
                        # print(chunk_request.json())
                        data[request_id] = chunk_request.json()["ChunkContent"]
                        break
                    else:
                        print(f"[{chunk_id}]The get_chunk request failed with status code {chunk_request.status_code}")
                number_of_retries += 1
            if not get_chunk_is_successful:
                print(f"FATAL, get_chunk() failed, please see above logs to see which chunk_id failed... read_file() for file_name={file_name} failed...")
                return ""
        # add to the data
        # data += chunk_data
    return ''.join(data)[position - previous_chunk_position: position + size - previous_chunk_position]

class CreateFileLogger:
    def __init__(self, parent_directory):
        self.parent_directory = parent_directory
        self.chunkserver_files = {}
    def initiate(self, chunk_id):
        self.chunkserver_files[chunk_id] = {}
    def mark_connected_to_chunkserver(self, chunk_id, chunk_server_session_id):
        mark_file_path = os.path.join(self.parent_directory, chunk_id, f"chunkserver_{chunk_server_session_id}")
        if self.chunkserver_files[chunk_id].get(chunk_server_session_id) is None:
            try:
                marker_file = open(mark_file_path, "w")
            except FileNotFoundError:
                # Create the directories
                os.makedirs(os.path.dirname(mark_file_path))
                marker_file = open(mark_file_path, "w")
            self.chunkserver_files[chunk_id][chunk_server_session_id] = marker_file
        self.chunkserver_files[chunk_id][chunk_server_session_id].write(f"{chunk_id} successfully connected to the chunkserver")
    def mark_chunk_written_to_chunkserver(self, chunk_id, chunk_server_session_id):
        mark_file_path = os.path.join(self.parent_directory, chunk_id, f"chunkserver_{chunk_server_session_id}")
        if self.chunkserver_files[chunk_id].get(chunk_server_session_id) is None:
            try:    
                marker_file = open(mark_file_path, "w")
            except FileNotFoundError:
                # Create the directories
                os.makedirs(os.path.dirname(mark_file_path))
                marker_file = open(mark_file_path, "w")
            
            self.chunkserver_files[chunk_id][chunk_server_session_id] = marker_file
        self.chunkserver_files[chunk_id][chunk_server_session_id].write(f"{chunk_id} successfully replicated chunk to the chunkserver")
    def close(self, chunk_id):
        # Close all the files
        for marker_file in self.chunkserver_files[chunk_id].values():
            marker_file.close()
    def clear_logs(self, chunk_id):
        self.close(chunk_id)
        chunk_id_directory_path = os.path.join(self.parent_directory, chunk_id)
        os.removedirs(chunk_id_directory_path)
        

    

def create_file(file_name, size, stream):
    print("The chunk size is ", client_config["gfs_chunk_size"])
    number_of_requests = int(size // client_config["gfs_chunk_size"]) + 1
    # data = "*" * size
    # TODO: parallelize this for loop
    create_file_logger = CreateFileLogger(client_config["LOG_DIRECTORY"])
    for request_id in range(number_of_requests):
        get_chunk_id_request = requests.post(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_id"), json={"file_name": file_name, "chunk_offset": str(request_id)})
        if get_chunk_id_request.status_code == 503:
            # The mainserver is busy processing another create_file request from server
            print(f"FATAL: Some other client beat you to it! Please wait for some time and raise an update_file request if you still want to append to the file, or a read_file request to check the contents of the file {file_name}")
        elif get_chunk_id_request.status_code == 409:
            print(f"FATAL: The file {file_name} already exists!")
        elif get_chunk_id_request.status_code == 200:
            chunk_id = get_chunk_id_request.json()["chunk_id"]
            create_file_logger.initiate(chunk_id)
            print(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"))
            # print({"chunk_id": chunk_id, "request_type": "write"})
            get_chunk_servers_json = requests.post(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"), json={"chunk_id": chunk_id}).json()
            # chunk_id = get_chunk_servers_json["chunk_id"]
            print("The get_chunk_servers_json is", get_chunk_servers_json)
            chunk_servers = get_chunk_servers_json["chunk_servers"]               
            print(f"Chunk server list is {chunk_servers}")
            # connect to the chunkserver in a fault tolerant way
            active_chunk_server = None 
            number_of_retries = 0
            write_chunk_is_successful = False
            write_chunkserver_status = [False] * len(chunk_servers)
            for chunk_server_idx, chunk_server in enumerate(chunk_servers):
                    if chunk_server_is_live(chunk_server):
                        chunk_server_heartbeat_request = requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat"))
                        print(urljoin(chunk_server["ipv4_address"], "heartbeat"))
                        if chunk_server_heartbeat_request.status_code == 200:
                            active_chunk_server = chunk_server
                        create_file_logger.mark_connected_to_chunkserver(chunk_id=chunk_id, chunk_server_session_id=chunk_server["session_id"])
                        while number_of_retries < client_config["GET_CHUNK_NUMBER_OF_RETRIES"]:
                                
                            # if connected_with_chunk_server == False:
                            #     # Return an error and fail this request
                            #     print("""Could not connect with any of the chunk_servers...\nHere is a list of the chunk_servers...""")
                            #     print(chunk_servers)
                            # else:
                            chunk_request = requests.post(urljoin(active_chunk_server["ipv4_address"], "write_chunk"), json={"chunk_id": chunk_id, "stream": stream[request_id * client_config["gfs_chunk_size"]: (request_id + 1) * client_config["gfs_chunk_size"]]})
                            if chunk_request.status_code == 200:
                                print(f"[{chunk_id}]write_chunk request is successful")
                                # write_chunk_is_successful = True
                                write_chunkserver_status[chunk_server_idx] = True
                                create_file_logger.mark_chunk_written_to_chunkserver(chunk_id=chunk_id, chunk_server_session_id=chunk_server["session_id"])
                                break
                            else:
                                print(f"[{chunk_id}]The write_chunk request failed with status code {chunk_request.status_code}")
                            number_of_retries += 1
                    else:
                        pass
            
            write_chunk_is_successful = reduce(lambda x, y: x and y, write_chunkserver_status)
            if not write_chunk_is_successful:
                print(f"FATAL, write_chunk() failed, please see above logs to see which chunk_id failed... create_file() for file_name={file_name} failed...")
                # TODO: Clear logs for this chunk_id
                create_file_logger.clear_logs(chunk_id)
                return
    
def update_file(file_name, position, size, updated_stream):
    pass 

connect()
stream = "Hello World blah-blah" * 1000000
create_file(f"blah-blah-{sys.argv[2]}.txt", len(stream), stream)
# read_file(f"blah-blah-{sys.argv[2]}.txt", 7, 200)