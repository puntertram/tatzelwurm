# This cleans up all the chunk files created and resets the state back to the original state
rm data_store/chunkserver_one/*
rm data_store/chunkserver_two/*
rm data_store/chunkserver_three/*

rm testing/output/*

# rm mainserver/chunk_server_map_audit.log
rm mainserver/file_name_to_chunk_id_map_audit.log
rm mainserver/persistent_hashmap_0_audit.log
rm mainserver/persistent_hashmap_1_audit.log
rm mainserver/persistent_hashmap_2_audit.log

# touch mainserver/chunk_server_map_audit.log
touch mainserver/file_name_to_chunk_id_map_audit.log
touch mainserver/persistent_hashmap_0_audit.log
touch mainserver/persistent_hashmap_1_audit.log
touch mainserver/persistent_hashmap_2_audit.log
