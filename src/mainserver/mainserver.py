from flask import Flask, request
import json  
from uuid import uuid4
import sys 
import os
import random 
# import atexit
from persistent_hashmap import PersistentHashMap

MAIN_SERVER_CONFIG_FILE_PATH = sys.argv[1]
mainserver_config = json.load(open(MAIN_SERVER_CONFIG_FILE_PATH))
chunk_id_to_chunk_server_maps = {}
chunk_server_map = None
file_name_to_chunk_id_map = None

def init():
    # initialize a list of data structures we need
    global chunk_id_to_chunk_server_maps
    global chunk_server_map
    global file_name_to_chunk_id_map
    replication_factor = mainserver_config["REPLICATION_FACTOR"]
    chunk_id_to_chunk_server_maps = {}
    chunk_server_map = {}
    for i in range(replication_factor):
        map_name = f"persistent_hashmap_{i}"
        # If there exists a WAL file, then recreate the hashmap
        chunk_id_to_chunk_server_maps[map_name] = PersistentHashMap(map_name, os.path.join(mainserver_config["PERSISTENT_HASHMAP_WAL_DIRECTORY"], f"{map_name}_audit.log"), mainserver_config)
    chunk_server_map = PersistentHashMap("chunk_server_map", os.path.join(mainserver_config["PERSISTENT_HASHMAP_WAL_DIRECTORY"], "chunk_server_map_audit.log"), mainserver_config)
    file_name_to_chunk_id_map = PersistentHashMap("file_name_to_chunk_id_map", os.path.join(mainserver_config["PERSISTENT_HASHMAP_WAL_DIRECTORY"], "file_name_to_chunk_id_map_audit.log"), mainserver_config)

init()
app = Flask(__name__)

@app.route("/")
def handle_base_url():
    return "This is the gfs mainserver..."

@app.route("/heartbeat", methods=["GET", "POST"])
def heartbeat_request():
    if request.method == "GET":
        return json.dumps({
            "message": "The gfs mainserver is alive and healthy...",
            "chunk_size": mainserver_config["GFS_CHUNK_SIZE"]
        })
    else:
        ip_address = request.json.get("ip_address")
        print(f"[heartbeat_request] Recieved request with ip_address->{ip_address}")
        if ip_address in chunk_server_map.dict.values():
            # This chunkserver is already registered with the mainserver
            chunk_server_session_id = None 
            for chunk_server_map_item in chunk_server_map.dict.items():
                if chunk_server_map_item[1] == ip_address:
                    chunk_server_session_id = chunk_server_map_item[0]
            print(f"[heartbeat_request] This chunkserver is already registered with the mainserver with session id {chunk_server_session_id}")
        else:
            # This chunkserver is not registered with the mainserver
            chunk_server_session_id = str(uuid4())
            chunk_server_map.put(chunk_server_session_id, ip_address)
        return json.dumps({
            "message": "The gfs mainserver is alive and healthy...",
            "chunk_size": 128,
            "session_id": chunk_server_session_id
        })
     

@app.route("/get_chunk_id")
def get_chunk_id():
    file_name = request.args.get("file_name")
    chunk_offset = request.args.get("chunk_offset")
    print(f"The file_name is {file_name}, and the chunk_offset is {chunk_offset}")
    chunk_id = file_name_to_chunk_id_map.get(f"{file_name}, {chunk_offset}")
    if chunk_id is None:
        # This is a write request, create a new chunk_id
        chunk_id = str(uuid4())
        file_name_to_chunk_id_map.put(f"{file_name}, {chunk_offset}", chunk_id)
    return json.dumps({
        "chunk_id": chunk_id
    })

@app.route("/get_chunk_servers")
def get_chunk_servers():
    chunk_id = request.args.get("chunk_id")
    request_type = request.args.get("request_type")
    print(f"The chunk id is {chunk_id}, request_type is {request_type}")
    if request_type == "read":
        chunk_servers = []
        for chunk_id_to_chunk_server_map in chunk_id_to_chunk_server_maps.values():
            chunk_server_session_id = chunk_id_to_chunk_server_map.get(chunk_id)
            chunk_server_ipv4_address = chunk_server_map.get(chunk_server_session_id)
            chunk_servers.append({
                "session_id": chunk_server_session_id,
                "ipv4_address": chunk_server_ipv4_address
            })
        return json.dumps({
            "chunk_servers": chunk_servers
        }) 
    elif request_type == "write":
        chunk_servers = []
        # randomly choose 2 chunkservers out of chunkserver_map
        items = random.sample(list(chunk_server_map.dict.items()), k=min(mainserver_config["REPLICATION_FACTOR"], len(chunk_server_map.dict.items())))
        for idx, item in enumerate(items):
            chunk_servers.append({
                "session_id": item[0],
                "ipv4_address": item[1]
            })

            chunk_id_to_chunk_server_maps[f"persistent_hashmap_{idx}"].put(chunk_id, item[0])    
        return json.dumps({
            "chunk_servers": chunk_servers
        }) 
    else:
        return f"Unidentified request_type {request_type}", 500


if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=mainserver_config["PORT"])




