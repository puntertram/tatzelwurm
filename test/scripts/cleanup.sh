# This cleans up all the chunk files created and resets the state back to the original state
cd /Users/puneeth/Documents/software/side-projects/gfs-clone
rm data_store/chunkserver_one/*
rm data_store/chunkserver_two/*
rm data_store/chunkserver_three/*

rm test/output/*

rm -rf client_meta/*

# rm mainserver_meta/chunk_server_map_audit.log
rm mainserver_meta/file_name_to_chunk_id_map_audit.log
rm mainserver_meta/persistent_hashmap_0_audit.log
rm mainserver_meta/persistent_hashmap_1_audit.log
rm mainserver_meta/persistent_hashmap_2_audit.log

# touch mainserver_meta/chunk_server_map_audit.log
touch mainserver_meta/file_name_to_chunk_id_map_audit.log
touch mainserver_meta/persistent_hashmap_0_audit.log
touch mainserver_meta/persistent_hashmap_1_audit.log
touch mainserver_meta/persistent_hashmap_2_audit.log
