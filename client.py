import requests
import sys 
import json
from urllib.parse import urljoin

CLIENT_CONFIG_FILE_PATH = sys.argv[1]
client_config = json.load(open(CLIENT_CONFIG_FILE_PATH))

def connect():
    # get the chunk size
    # setup a connection with the master server
    assert requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "heartbeat")).status_code == 200, "Connection to the GFS server failed..."
    print("Connection to master succeeded")
    chunk_size_request_json = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "heartbeat")).json()
    client_config["gfs_chunk_size"] = chunk_size_request_json["chunk_size"] 

def chunk_server_is_live(chunk_server):
    if requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat")).status_code == 200:
        return True 
    else:
        return False

def read_file(file_name, position, size):
    number_of_requests = int(size // client_config["gfs_chunk_size"]) + 1
    data = ""
    for request_id in range(number_of_requests):
        # from the chunk size convert to a chunk offset in the file
        chunk_offset = int(position // client_config["gfs_chunk_size"])
        # get the chunk id, and the list of chunkservers to reach out to from the master
        chunk_id = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_id"), params={"file_name": file_name, "chunk_offset": chunk_offset}).json()["chunk_id"]       
        chunk_servers = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"), params={"chunk_id": chunk_id}).json()["chunk_servers"]               
        # connect to the chunkservers in a fault tolerant way and get the chunk contents
        connected_with_chunk_server = False 
        chunk_server_session_id = None 
        for chunk_server_idx, chunk_server in enumerate(chunk_servers):
            if chunk_server_is_live(chunk_server):
                chunk_server_session_id = requests.get(urljoin(chunk_server["ipv4_address"], "connect"))
                connected_with_chunk_server = True 
                break 
        if connected_with_chunk_server == False:
            # Return an error and fail this request
            pass
        else:
            pass
        # add to the data
        # data += chunk_data
    return data

def create_file(file_name, size):
    pass 


def update_file(file_name, position, size, updated_stream):
    pass 

connect()
read_file("blah-blah.txt", 1, 10)