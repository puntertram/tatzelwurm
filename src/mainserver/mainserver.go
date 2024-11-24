package mainserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"tatzelwurm/utils/persistent_hashmap"
)

type MAIN_SERVER_CONFIG struct {
	PERSISTENT_HASHMAP_WAL_DIRECTORY string `json:"PERSISTENT_HASHMAP_WAL_DIRECTORY"`
	REPLICATION_FACTOR               int    `json:"REPLICATION_FACTOR"`
	PORT                             int    `json:"PORT"`
	GFS_CHUNK_SIZE                   int    `json:"GFS_CHUNK_SIZE"`
}

type MainServerPersistentHashmap struct {
}

func main() {
	var MAIN_SERVER_CONFIG_FILE_PATH = os.Args[1]
	mainServerConfigFileData, err := os.ReadFile(MAIN_SERVER_CONFIG_FILE_PATH)
	if err != nil {
		fmt.Println("Error opening file at path ", MAIN_SERVER_CONFIG_FILE_PATH)
	}
	var mainServerConfigFileJson MAIN_SERVER_CONFIG
	err1 := json.Unmarshal(mainServerConfigFileData, &mainServerConfigFileJson)
	if err1 != nil {
		fmt.Println("Error parsing the json in the file at path ", MAIN_SERVER_CONFIG_FILE_PATH)
	}
	// Data structures used by mainserver
	// var chunk_id_to_chunk_server_map

	var portNumber = mainServerConfigFileJson.PORT
	if err := http.ListenAndServe("localhost:"+strconv.Itoa(portNumber), nil); err != nil {
		fmt.Println("Could not start http server on port ", portNumber, err)
	}
}

