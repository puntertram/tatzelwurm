package model

import (
	"fmt"
	"strconv"
	"strings"
	persistent_hashmap "tatzelwurm/utils"
)

type CHUNK_SERVER_CONFIG struct {
	PERSISTENT_HASHMAP_WAL_DIRECTORY string `json:"PERSISTENT_HASHMAP_WAL_DIRECTORY"`
	PORT                             int    `json:"PORT"`
	DOMAIN                           string `json:"DOMAIN"`
	GFS_SERVER_INFO_IPV4_ADDRESS     string `json:"GFS_SERVER_INFO_IPV4_ADDRESS"`
	DATA_DIRECTORY                   string `json:"DATA_DIRECTORY"`
}

type ChunkServerToChunkIdMapValue struct {
	Is_replicated bool
	Is_dirty      bool
}
type ChunkServerPersistentHashmap struct {
	*persistent_hashmap.PersistentHashmap
	Config CHUNK_SERVER_CONFIG
}

func (c *ChunkServerPersistentHashmap) Get(key string) (ChunkServerToChunkIdMapValue, bool) {
	value, ok := c.PersistentHashmap.Get(key)
	values := strings.Split(value, ",")
	var return_value ChunkServerToChunkIdMapValue
	// parse value into return_value
	if ok {
		is_replicated_value, err := strconv.ParseBool(strings.TrimSpace(values[0]))
		if err != nil {
			return return_value, false
		}
		is_dirty_value, err := strconv.ParseBool(strings.TrimSpace(values[1]))
		if err != nil {
			return return_value, false
		}
		return_value = ChunkServerToChunkIdMapValue{
			Is_replicated: is_replicated_value,
			Is_dirty:      is_dirty_value,
		}
	}
	return return_value, ok
}

func (c *ChunkServerPersistentHashmap) Put(key string, value ChunkServerToChunkIdMapValue) bool {
	value_as_string := fmt.Sprintf("%v, %v", value.Is_replicated, value.Is_replicated)
	return c.PersistentHashmap.Put(key, value_as_string)
}
