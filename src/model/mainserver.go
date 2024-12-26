package model

import (
	"fmt"
	"strconv"
	"strings"
	persistent_hashmap "tatzelwurm/utils"
)

type MAIN_SERVER_CONFIG struct {
	PERSISTENT_HASHMAP_WAL_DIRECTORY string `json:"PERSISTENT_HASHMAP_WAL_DIRECTORY"`
	REPLICATION_FACTOR               int    `json:"REPLICATION_FACTOR"`
	PORT                             int    `json:"PORT"`
	GFS_CHUNK_SIZE                   int    `json:"GFS_CHUNK_SIZE"`
}

type PROCESS_HEARTBEAT_POST_REQUEST_BODY struct {
	IP_ADDRESS string `json:"IP_ADDRESS"`
}

type MainServerPersistentHashmap struct {
	Config MAIN_SERVER_CONFIG
	*persistent_hashmap.PersistentHashmap
}

type ChunkIdToChunkServerPersistentHashmap struct {
	*MainServerPersistentHashmap
}
type ChunkIdToChunkServerPersistentHashmapValue struct {
	Chunk_server_session_id string
	Is_replicated           bool
}

func (c *ChunkIdToChunkServerPersistentHashmap) Get(key string) (ChunkIdToChunkServerPersistentHashmapValue, bool) {
	value, ok := c.PersistentHashmap.Get(key)
	values := strings.Split(value, ",")
	var return_value ChunkIdToChunkServerPersistentHashmapValue
	if ok {
		is_replicated_value, err := strconv.ParseBool(strings.TrimSpace(values[1]))
		if err != nil {
			return return_value, false
		}
		return_value = ChunkIdToChunkServerPersistentHashmapValue{
			Chunk_server_session_id: strings.TrimSpace(values[0]),
			Is_replicated:           is_replicated_value,
		}
	}
	return return_value, ok
}

func (c *ChunkIdToChunkServerPersistentHashmap) Put(key string, value ChunkIdToChunkServerPersistentHashmapValue) bool {
	value_string := fmt.Sprintf("%s, %v", value.Chunk_server_session_id, value.Is_replicated)
	return c.PersistentHashmap.Put(key, value_string)
}

type ChunkServerInformationPersistentHashmap struct {
	*MainServerPersistentHashmap
}

type ChunkServerPersistentInformationHashmapValue struct {
	Ipv4_address string
}

func (c *ChunkServerInformationPersistentHashmap) Get(key string) (ChunkServerPersistentInformationHashmapValue, bool) {
	value, ok := c.PersistentHashmap.Get(key)
	return_value := ChunkServerPersistentInformationHashmapValue{
		Ipv4_address: strings.TrimSpace(value),
	}
	return return_value, ok
}
func (c *ChunkServerInformationPersistentHashmap) Put(key string, value ChunkServerPersistentInformationHashmapValue) bool {
	value_string := fmt.Sprintf("%s", value.Ipv4_address)
	return c.PersistentHashmap.Put(key, value_string)
}

type FileNameToChunkIdPersistentHashmap struct {
	*MainServerPersistentHashmap
}

type FileNameToChunkIdPersistentHashmapValue struct {
	Chunk_id string
}

func (f *FileNameToChunkIdPersistentHashmap) Get(key string) (FileNameToChunkIdPersistentHashmapValue, bool) {
	value, ok := f.PersistentHashmap.Get(key)
	return_value := FileNameToChunkIdPersistentHashmapValue{
		Chunk_id: strings.TrimSpace(value),
	}
	return return_value, ok
}

func (f *FileNameToChunkIdPersistentHashmap) Put(key string, value FileNameToChunkIdPersistentHashmapValue) bool {
	value_string := fmt.Sprintf("%s", value.Chunk_id)
	return f.PersistentHashmap.Put(key, value_string)
}
