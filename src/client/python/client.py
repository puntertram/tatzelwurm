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
    print(chunk_server)
    if requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat")).status_code == 200:
        return True 
    else:
        return False

def read_file(file_name, position, size):
    next_chunk_end_position = ((size // client_config["gfs_chunk_size"]) + 1) * client_config["gfs_chunk_size"]
    if next_chunk_end_position <= position + size:
        number_of_requests = 2 + ((position + size - next_chunk_end_position) // client_config["gfs_chunk_size"])
    else:
        number_of_requests = 1
    data = ["*"] * number_of_requests
    print("The number of requests is", number_of_requests)
    for request_id in range(number_of_requests):
        # from the chunk size convert to a chunk offset in the file
        chunk_offset = int(position // client_config["gfs_chunk_size"]) + request_id
        # get the chunk id, and the list of chunkservers to reach out to from the master
        chunk_id = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_id"), params={"file_name": file_name, "chunk_offset": chunk_offset}).json()["chunk_id"]       
        chunk_servers = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"), params={"chunk_id": chunk_id, "request_type": "read"}).json()["chunk_servers"]               
        print(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"))
        print({"chunk_id": chunk_id, "request_type": "read"})
        # connect to the chunkservers in a fault tolerant way and get the chunk contents
        connected_with_chunk_server = False 
        active_chunk_server = None 
        number_of_retries = 0
        get_chunk_is_successful = False
        while number_of_retries < client_config["GET_CHUNK_NUMBER_OF_RETRIES"]:
            print(chunk_servers)
            for chunk_server_idx, chunk_server in enumerate(chunk_servers):
                if chunk_server_is_live(chunk_server):
                    chunk_server_heartbeat_request = requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat"))
                    if chunk_server_heartbeat_request.status_code == 200:
                        active_chunk_server = chunk_server
                    connected_with_chunk_server = True 
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
                    data[request_id] = chunk_request.json()["chunk_content"]
                    break
                else:
                    print(f"[{chunk_id}]The get_chunk request failed with status code {chunk_request.status_code}")
            number_of_retries += 1
        if not get_chunk_is_successful:
            print(f"FATAL, get_chunk() failed, please see above logs to see which chunk_id failed... read_file() for file_name={file_name} failed...")
            return ""
        # add to the data
        # data += chunk_data
    return ''.join(data)[position: position + size]

def create_file(file_name, size, stream):
    number_of_requests = int(size // client_config["gfs_chunk_size"]) + 1
    # data = "*" * size
    # TODO: parallelize this for loop
    for request_id in range(number_of_requests):
        get_chunk_id_request = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_id"), params={"file_name": file_name, "chunk_offset": request_id})
        if get_chunk_id_request.status_code == 503:
            # The mainserver is busy processing another create_file request from server
            print(f"FATAL: Some other client beat you to it! Please wait for some time and raise an update_file request if you still want to append to the file, or a read_file request to check the contents of the file {file_name}")
        elif get_chunk_id_request.status_code == 409:
            print(f"FATAL: The file {file_name} already exists!")
        elif get_chunk_id_request.status_code == 200:
            chunk_id = get_chunk_id_request.json()["chunk_id"]
            print(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"))
            print({"chunk_id": chunk_id, "request_type": "write"})
            get_chunk_servers_json = requests.get(urljoin(client_config["gfs_server_info"]["ipv4_address"], "get_chunk_servers"), params={"chunk_id": chunk_id, "request_type": "write"}).json()
            # chunk_id = get_chunk_servers_json["chunk_id"]
            print("The get_chunk_servers_json is", get_chunk_servers_json)
            chunk_servers = get_chunk_servers_json["chunk_servers"]               
            print(f"Chunk server list is {chunk_servers}")
            # connect to the chunkserver in a fault tolerant way
            connected_with_chunk_server = False 
            active_chunk_server = None 
            number_of_retries = 0
            write_chunk_is_successful = False
            while number_of_retries < client_config["GET_CHUNK_NUMBER_OF_RETRIES"]:
                for chunk_server_idx, chunk_server in enumerate(chunk_servers):
                    if chunk_server_is_live(chunk_server):
                        chunk_server_heartbeat_request = requests.get(urljoin(chunk_server["ipv4_address"], "heartbeat"))
                        print(urljoin(chunk_server["ipv4_address"], "heartbeat"))
                        if chunk_server_heartbeat_request.status_code == 200:
                            active_chunk_server = chunk_server
                        connected_with_chunk_server = True 
                        break 
                if connected_with_chunk_server == False:
                    # Return an error and fail this request
                    print("""Could not connect with any of the chunk_servers...\nHere is a list of the chunk_servers...""")
                    print(chunk_servers)
                else:
                    chunk_request = requests.post(urljoin(active_chunk_server["ipv4_address"], "write_chunk"), json={"chunk_id": chunk_id, "stream": stream[request_id * client_config["gfs_chunk_size"]: (request_id + 1) * client_config["gfs_chunk_size"]]})
                    if chunk_request.status_code == 200:
                        print(f"[{chunk_id}]write_chunk request is successful")
                        write_chunk_is_successful = True
                        break
                    else:
                        print(f"[{chunk_id}]The write_chunk request failed with status code {chunk_request.status_code}")
                number_of_retries += 1
            if not write_chunk_is_successful:
                print(f"FATAL, write_chunk() failed, please see above logs to see which chunk_id failed... create_file() for file_name={file_name} failed...")
                return
    
def update_file(file_name, position, size, updated_stream):
    pass 

connect()
stream = "Hello World blah-blah" * 1000000
create_file(f"blah-blah-{sys.argv[2]}.txt", len(stream), stream)
# print(read_file("blah-blah-2.txt", 22, 10000))