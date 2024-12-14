package chunkserver

import (
	"fmt"
	persistent_hashmap "tatzelwurm/utils"
)

type CHUNK_SERVER_CONFIG struct {
	PERSISTENT_HASHMAP_WAL_DIRECTORY string `json:"PERSISTENT_HASHMAP_WAL_DIRECTORY"`
	REPLICATION_FACTOR               int    `json:"REPLICATION_FACTOR"`
	PORT                             int    `json:"PORT"`
	GFS_CHUNK_SIZE                   int    `json:"GFS_CHUNK_SIZE"`
}

type ChunkServerPersistentHashmap struct {
	persistent_hashmap.PersistentHashmap
	config CHUNK_SERVER_CONFIG
}

func Run(chunkserver_config_file_path string) {
	fmt.Println("Running as the chunkserver...")
}
