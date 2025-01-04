mkdir -p data_store
mkdir -p data_store/chunkserver_one
mkdir -p data_store/chunkserver_two
mkdir -p data_store/chunkserver_three

mkdir -p client_meta

mkdir -p chunkserver_meta/chunkserver_one
mkdir -p chunkserver_meta/chunkserver_two
mkdir -p chunkserver_meta/chunkserver_three
touch chunkserver_meta/chunkserver_one/chunk_server_map_audit.log
touch chunkserver_meta/chunkserver_two/chunk_server_map_audit.log
touch chunkserver_meta/chunkserver_three/chunk_server_map_audit.log

mkdir -p mainserver_meta
touch mainserver_meta/chunk_server_map_audit.log
touch mainserver_meta/file_name_to_chunk_id_map_audit.log
touch mainserver_meta/persistent_hashmap_0_audit.log
touch mainserver_meta/persistent_hashmap_1_audit.log
touch mainserver_meta/persistent_hashmap_2_audit.log
