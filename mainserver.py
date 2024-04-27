from flask import Flask, request
import json  
from uuid import uuid4
app = Flask(__name__)

@app.route("/")
def handle_base_url():
    return "This is the gfs mainserver..."

@app.route("/heartbeat")
def heartbeat_request():
    return json.dumps({
        "message": "The gfs mainserver is alive and healthy...",
        "chunk_size": 128
    }) 

@app.route("/get_chunk_id")
def get_chunk_id():
    file_name = request.args.get("file_name")
    chunk_offset = request.args.get("chunk_offset")
    print(f"The file_name is {file_name}, and the chunk_offset is {chunk_offset}")
    return json.dumps({
        "chunk_id": str(uuid4())
    })







