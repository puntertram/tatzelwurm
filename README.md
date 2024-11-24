# Start GFS
1. Start the GFS mainserver: go run src/mainserver/mainserver.go /Users/puneeth/Documents/software/side-projects/gfs-clone/config/mainserver/main_server.json
2. Start the chunkservers: python3 chunkserver.py /Users/puneeth/Documents/software/side-projects/gfs-clone/chunk_server_config.json
3. Start the example client: python3 client.py /Users/puneeth/Documents/software/side-projects/gfs-clone/client_config.json 


# Documentation