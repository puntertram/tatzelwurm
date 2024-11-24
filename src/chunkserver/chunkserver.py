from flask import Flask, request
import json  
from uuid import uuid4
import requests
import sys 
import json
import os 
from urllib.parse import urljoin
import socket

app = Flask(__name__)
CHUNKSERVER_CONFIG_FILE_PATH = sys.argv[1]
chunkserver_config = json.load(open(CHUNKSERVER_CONFIG_FILE_PATH))

def connect():
    # get the chunk size
    # setup a connection with the master server
    hostname = socket.gethostname()
    # print(hostname)
    local_ip = f"http://localhost:{chunkserver_config['chunk_server_info']['port']}"
    # local_ip = socket.gethostbyname(hostname)
    heartbeat_request = requests.post(urljoin(chunkserver_config["gfs_server_info"]["ipv4_address"], "heartbeat"), json={"ip_address": local_ip})
    print(urljoin(chunkserver_config["gfs_server_info"]["ipv4_address"], "heartbeat"))
    print(heartbeat_request.status_code)
    assert heartbeat_request.status_code == 200, "Connection to the GFS server failed..."
    print("Connection to master succeeded")
    chunk_size_request_json = heartbeat_request.json()
    chunkserver_config["gfs_chunk_size"] = chunk_size_request_json["chunk_size"] 


connect()

@app.route("/heartbeat")
def heartbeat():
    return "Chunkserver is alive..."

@app.route("/get_chunk")
def get_chunk():
    chunk_id = request.args.get("chunk_id")
    print(f"The chunk_id is {chunk_id}")
    if os.path.exists(os.path.join(chunkserver_config["chunk_server_info"]["data_directory"], chunk_id)):
        response = {
            "chunk_content": ""
        }
        with open(os.path.join(chunkserver_config["chunk_server_info"]["data_directory"], chunk_id), "r") as fil:
            response = {
                "chunk_content" : "\n".join(fil.readlines())
            } 
        return response
    else:
        return f"chunk_id {chunk_id} not found", 500

@app.route("/write_chunk", methods=["POST"])
def write_chunk():
    request_json = request.json
    chunk_id = request_json.get("chunk_id")
    stream = request_json.get("stream")
    # print(f"The chunk_id {chunk_id}, the stream is {stream}")
    try:
        with open(os.path.join(chunkserver_config["chunk_server_info"]["data_directory"], chunk_id), "a") as fil:
            fil.write(stream)
    finally:
        pass
    return "Write sucessful", 200 


if __name__ == "__main__":
    app.run(debug=False, host="127.0.0.1", port=chunkserver_config["chunk_server_info"]["port"])


