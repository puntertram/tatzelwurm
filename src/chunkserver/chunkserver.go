package chunkserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	persistent_hashmap "tatzelwurm/utils"
	"time"
)

type CHUNK_SERVER_CONFIG struct {
	PERSISTENT_HASHMAP_WAL_DIRECTORY string `json:"PERSISTENT_HASHMAP_WAL_DIRECTORY"`
	PORT                             int    `json:"PORT"`
	DOMAIN                           string `json:"DOMAIN"`
	GFS_SERVER_INFO_IPV4_ADDRESS     string `json:"GFS_SERVER_INFO_IPV4_ADDRESS"`
	DATA_DIRECTORY                   string `json:"DATA_DIRECTORY"`
}

type ChunkServerToChunkIdMapValue struct {
	is_replicated bool
	is_dirty      bool
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
		is_replicated_value, err := strconv.ParseBool(values[0])
		if err != nil {
			return return_value, false
		}
		is_dirty_value, err := strconv.ParseBool(values[1])
		if err != nil {
			return return_value, false
		}
		return_value = ChunkServerToChunkIdMapValue{
			is_replicated: is_replicated_value,
			is_dirty:      is_dirty_value,
		}
	}
	return return_value, ok
}

func (c *ChunkServerPersistentHashmap) Put(key string, value ChunkServerToChunkIdMapValue) bool {
	value_as_string := fmt.Sprintf("%v, %v", value.is_replicated, value.is_replicated)
	return c.PersistentHashmap.Put(key, value_as_string)
}

var chunkServerConfigFileJson CHUNK_SERVER_CONFIG
var chunk_server_to_chunk_id_map *ChunkServerPersistentHashmap = nil

func connect(chunkServerConfigFileJson CHUNK_SERVER_CONFIG) error {
	var error error
	local_ip := fmt.Sprintf("http://%s:%d", chunkServerConfigFileJson.DOMAIN, chunkServerConfigFileJson.PORT)
	heartbeat_request_url := fmt.Sprintf("%s/heartbeat", chunkServerConfigFileJson.GFS_SERVER_INFO_IPV4_ADDRESS)
	heartbeat_request_data := map[string]string{
		"ip_address": local_ip,
	}
	heartbeat_request_json_data, _ := json.Marshal(heartbeat_request_data)
	heartbeat_response, err := http.Post(heartbeat_request_url, "application/json", bytes.NewBuffer(heartbeat_request_json_data))
	if err != nil {
		print("Could not connect to the mainserver... more details", err)
	} else {
		if heartbeat_response.StatusCode == http.StatusOK {
			print("Connected to the mainserver successfully")
		} else {
			print("Got status code ", heartbeat_response.StatusCode, "while connecting to the mainserver... Aborting")
			error = fmt.Errorf("error while connecting to the mainserver... aborting")
		}
	}
	return error
}

func processBase(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("This is the chunkserver..."))
}

func processHeartBeat(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Chunkserver is alive..."))
}

func fileExists(filePath string) bool {
	// Use os.Stat to get file information
	_, err := os.Stat(filePath)
	// If err is nil, the file exists; if err is not nil, check if it's a "not found" error
	return !os.IsNotExist(err)
}

func processGetChunk(w http.ResponseWriter, r *http.Request) {
	type GetChunkResponseModel struct {
		Description  string `json:description`
		ChunkContent string `json:chunk_content`
	}
	var response GetChunkResponseModel
	if r.Method == http.MethodGet {
		chunk_id := r.URL.Query().Get("chunk_id")
		chunk_file_path := fmt.Sprintf("%s/%s", chunkServerConfigFileJson.DATA_DIRECTORY, chunk_id)
		if fileExists(chunk_file_path) {
			chunk_file_data, err := os.ReadFile(chunk_file_path)
			if err != nil {
				// fmt.Println("Error opening file at path ", chunkserver_config_file_path)
				w.WriteHeader(http.StatusInternalServerError)
				response = GetChunkResponseModel{
					Description:  fmt.Sprintf("Error opening file at path %s", chunk_file_path),
					ChunkContent: "",
				}
			} else {
				response = GetChunkResponseModel{
					Description:  "Fetched the chunk content successfully",
					ChunkContent: string(chunk_file_data),
				}
			}

		} else {
			w.WriteHeader(http.StatusNotFound)
			response = GetChunkResponseModel{
				Description:  "Cannot fetch Chunk content",
				ChunkContent: "",
			}
		}

	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		response = GetChunkResponseModel{
			Description:  "Method not supported",
			ChunkContent: "",
		}
	}
	json.NewEncoder(w).Encode(response)
}

func processWriteChunk(w http.ResponseWriter, r *http.Request) {
	type WriteChunkResponseModel struct {
		Description string `json:"description"`
	}
	var response WriteChunkResponseModel
	if r.Method == http.MethodPost {
		var request_body_json map[string]string
		json.NewDecoder(r.Body).Decode(&request_body_json)
		chunk_id := request_body_json["chunk_id"]
		chunk_stream := request_body_json["stream"]
		chunk_file_path := fmt.Sprintf("%s/%s", chunkServerConfigFileJson.DATA_DIRECTORY, chunk_id)
		println("The chunk_file_path is ", chunk_file_path)
		err := os.WriteFile(chunk_file_path, []byte(chunk_stream), 0644)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			print("[processWriteChunk] Encountered error while writing to file ", err.Error())
			response = WriteChunkResponseModel{
				Description: "Error Writing stream to file",
			}
		} else {
			old_value, _ := chunk_server_to_chunk_id_map.Get(chunk_id)
			chunk_server_to_chunk_id_map.Put(chunk_id, ChunkServerToChunkIdMapValue{
				is_replicated: true,
				is_dirty:      !old_value.is_replicated,
			})
			response = WriteChunkResponseModel{
				Description: fmt.Sprintf("%s contents successfully written", chunk_id),
			}
		}
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		response = WriteChunkResponseModel{
			Description: "Method not supported",
		}
	}
	json.NewEncoder(w).Encode(response)
}

func runServer(wg *sync.WaitGroup, chunkServerConfigFileJson CHUNK_SERVER_CONFIG) {
	defer wg.Done()
	var portNumber = chunkServerConfigFileJson.PORT
	if err := http.ListenAndServe("localhost:"+strconv.Itoa(portNumber), nil); err != nil {
		fmt.Println("Could not start http server on port ", portNumber, err)
	}
}
func syncWithMainServer(wg *sync.WaitGroup) {

	ticker := time.NewTicker(5 * time.Second) // Run every 5 seconds
	defer wg.Done()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// We go through the chunk_server_to_chunk_id_map values and sync the values that are dirty
			for key, _ := range chunk_server_to_chunk_id_map.HashMap {
				value, _ := chunk_server_to_chunk_id_map.Get(key)
				if value.is_dirty {
					fmt.Printf("[syncWithMainServer] relaying information about chunk_id %s to the mainserver about the value %v\n", key, value.is_replicated)

					sync_with_mainserver_request_url := fmt.Sprintf("%s/syncFromChunkServer", chunkServerConfigFileJson.GFS_SERVER_INFO_IPV4_ADDRESS)
					sync_with_mainserver_request_data := map[string]string{
						"chunk_id":      key,
						"is_replicated": "true",
					}
					sync_with_mainserver_request_json_data, _ := json.Marshal(sync_with_mainserver_request_data)
					sync_with_mainserver_response, err := http.Post(sync_with_mainserver_request_url, "application/json", bytes.NewBuffer(sync_with_mainserver_request_json_data))
					if err != nil {
						fmt.Printf("[syncWithMainServer] error while syncing the value with the mainserver, more details %s\n", err.Error())
					} else {
						if sync_with_mainserver_response.StatusCode == http.StatusOK {
							value.is_dirty = false
							// We are not bothered about the success of the Put function below,
							// As if the below call fails, then this key,value pair will be picked up again
							// and will be synced to the mainserver in the next run. No harmful changes
							// will occur except some additional http requests to the mainserver
							chunk_server_to_chunk_id_map.Put(key, value)
						} else {
							fmt.Printf("[syncWithMainServer] unexpected sync_with_mainserver_request status code %d\n", sync_with_mainserver_response.StatusCode)
						}
					}
				}
			}

		}
	}
}

func Run(chunkserver_config_file_path string) {
	fmt.Println("Running as the chunkserver...")
	chunkServerConfigFileData, err := os.ReadFile(chunkserver_config_file_path)
	if err != nil {
		fmt.Println("Error opening file at path ", chunkserver_config_file_path)
	}
	err1 := json.Unmarshal(chunkServerConfigFileData, &chunkServerConfigFileJson)
	if err1 != nil {
		fmt.Println("Error parsing the json in the file at path ", chunkserver_config_file_path)
	}
	err = connect(chunkServerConfigFileJson)
	if err != nil {
		print("Could not connect to the mainserver")
	}
	chunk_server_to_chunk_id_map = &ChunkServerPersistentHashmap{
		Config: chunkServerConfigFileJson,
		PersistentHashmap: &persistent_hashmap.PersistentHashmap{
			AuditLogFilePath: filepath.Join(chunkServerConfigFileJson.PERSISTENT_HASHMAP_WAL_DIRECTORY, "chunk_server_map_audit.log"),
			Namespace:        "chunk_server_to_chunk_id_map",
		},
	}

	http.HandleFunc("/", processBase)
	http.HandleFunc("/heartbeat", processHeartBeat)
	http.HandleFunc("/get_chunk", processGetChunk)
	http.HandleFunc("/write_chunk", processWriteChunk)

	var wg sync.WaitGroup
	go runServer(&wg, chunkServerConfigFileJson)
	go syncWithMainServer(&wg)

	wg.Wait()
}
