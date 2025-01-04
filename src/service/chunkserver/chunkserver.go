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
	"tatzelwurm/model"
	request_dto "tatzelwurm/model/api/request"
	response_dto "tatzelwurm/model/api/response"
	persistent_hashmap "tatzelwurm/utils"
	"time"
)

var chunkServerConfigFileJson model.CHUNK_SERVER_CONFIG
var chunk_server_to_chunk_id_map *model.ChunkServerPersistentHashmap = nil

func connect(chunkServerConfigFileJson model.CHUNK_SERVER_CONFIG) error {
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
	var response response_dto.GetChunkResponseModel
	if r.Method == http.MethodGet {
		chunk_id := r.URL.Query().Get("chunk_id")
		chunk_file_path := fmt.Sprintf("%s/%s", chunkServerConfigFileJson.DATA_DIRECTORY, chunk_id)
		if fileExists(chunk_file_path) {
			chunk_file_data, err := os.ReadFile(chunk_file_path)
			if err != nil {
				// fmt.Println("Error opening file at path ", chunkserver_config_file_path)
				w.WriteHeader(http.StatusInternalServerError)
				response = response_dto.GetChunkResponseModel{
					Description:  fmt.Sprintf("Error opening file at path %s", chunk_file_path),
					ChunkContent: "",
				}
			} else {
				response = response_dto.GetChunkResponseModel{
					Description:  "Fetched the chunk content successfully",
					ChunkContent: string(chunk_file_data),
				}
			}

		} else {
			w.WriteHeader(http.StatusNotFound)
			response = response_dto.GetChunkResponseModel{
				Description:  "Cannot fetch Chunk content",
				ChunkContent: "",
			}
		}

	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		response = response_dto.GetChunkResponseModel{
			Description:  "Method not supported",
			ChunkContent: "",
		}
	}
	json.NewEncoder(w).Encode(response)
}

func processWriteChunk(w http.ResponseWriter, r *http.Request) {
	var response response_dto.WriteChunkResponseModel
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
			response = response_dto.WriteChunkResponseModel{
				Description: "Error Writing stream to file",
			}
		} else {
			old_value, _ := chunk_server_to_chunk_id_map.Get(chunk_id)
			chunk_server_to_chunk_id_map.Put(chunk_id, model.ChunkServerToChunkIdMapValue{
				Is_replicated: true,
				Is_dirty:      !old_value.Is_replicated,
			})
			response = response_dto.WriteChunkResponseModel{
				Description: fmt.Sprintf("%s contents successfully written", chunk_id),
			}
		}
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		response = response_dto.WriteChunkResponseModel{
			Description: "Method not supported",
		}
	}
	json.NewEncoder(w).Encode(response)
}

func runServer(chunkServerConfigFileJson model.CHUNK_SERVER_CONFIG) {
	var portNumber = chunkServerConfigFileJson.PORT
	if err := http.ListenAndServe("localhost:"+strconv.Itoa(portNumber), nil); err != nil {
		fmt.Println("Could not start http server on port ", portNumber, err)
	}
}
func syncWithMainServer(ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
			// We go through the chunk_server_to_chunk_id_map values and sync the values that are dirty
			fmt.Printf("Syncing with MainServer...\n")
			sync_with_mainserver_request_url := fmt.Sprintf("%s/syncFromChunkServer", chunkServerConfigFileJson.GFS_SERVER_INFO_IPV4_ADDRESS)
			sync_with_mainserver_request_data := request_dto.SyncWithMainserverRequestModel{
				Chunk_list:   make([]request_dto.SyncWithMainserverRequestChunkListModel, 0),
				Ipv4_address: fmt.Sprintf("http://%s:%d", chunkServerConfigFileJson.DOMAIN, chunkServerConfigFileJson.PORT),
			}
			chunk_server_to_chunk_id_map.PersistentHashmap.Mu.RLock()
			for key, _ := range chunk_server_to_chunk_id_map.HashMap {
				value, _ := chunk_server_to_chunk_id_map.Get(key)
				if value.Is_dirty {
					fmt.Printf("[syncWithMainServer] relaying information about chunk_id %s to the mainserver about the value %v\n", key, value.Is_replicated)
					sync_with_mainserver_request_data.Chunk_list = append(sync_with_mainserver_request_data.Chunk_list, request_dto.SyncWithMainserverRequestChunkListModel{
						Chunk_id:      key,
						Is_replicated: true,
					})
				}
			}
			chunk_server_to_chunk_id_map.PersistentHashmap.Mu.RUnlock()
			sync_with_mainserver_request_json_data, _ := json.Marshal(sync_with_mainserver_request_data)
			sync_with_mainserver_response, err := http.Post(sync_with_mainserver_request_url, "application/json", bytes.NewBuffer(sync_with_mainserver_request_json_data))
			if err != nil {
				fmt.Printf("[syncWithMainServer] error while syncing the value with the mainserver, more details %s\n", err.Error())
			} else {
				if sync_with_mainserver_response.StatusCode == http.StatusOK {
					var sync_with_mainserver_response_data []response_dto.SyncWithMainserverResponseModel
					json.NewDecoder(sync_with_mainserver_response.Body).Decode(&sync_with_mainserver_response_data)
					for idx := range sync_with_mainserver_response_data {
						if sync_with_mainserver_response_data[idx].Status == model.ChunkSyncedSuccessfully {
							value, _ := chunk_server_to_chunk_id_map.Get(sync_with_mainserver_response_data[idx].Chunk_id)
							value.Is_dirty = false
							chunk_server_to_chunk_id_map.Put(strings.TrimSpace(sync_with_mainserver_response_data[idx].Chunk_id), value)
						} else if sync_with_mainserver_response_data[idx].Status == model.ChunkSyncFailed {
							// We are not bothered about the success of the Put function below,
							// As if the below call fails, then this key,value pair will be picked up again
							// and will be synced to the mainserver in the next run. No harmful changes
							// will occur except some additional http requests to the mainserver
						}
					}

				} else {
					fmt.Printf("[syncWithMainServer] unexpected sync_with_mainserver_request status code %d\n", sync_with_mainserver_response.StatusCode)
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
	chunk_server_to_chunk_id_map = &model.ChunkServerPersistentHashmap{
		Config: chunkServerConfigFileJson,
		PersistentHashmap: &persistent_hashmap.PersistentHashmap{
			AuditLogFilePath: filepath.Join(chunkServerConfigFileJson.PERSISTENT_HASHMAP_WAL_DIRECTORY, "chunk_server_map_audit.log"),
			Namespace:        "chunk_server_to_chunk_id_map",
		},
	}
	chunk_server_to_chunk_id_map.PersistentHashmap.Initialize()

	http.HandleFunc("/", processBase)
	http.HandleFunc("/heartbeat", processHeartBeat)
	http.HandleFunc("/get_chunk", processGetChunk)
	http.HandleFunc("/write_chunk", processWriteChunk)

	ticker := time.NewTicker(5 * time.Second) // Run every 5 seconds

	go runServer(chunkServerConfigFileJson)
	go syncWithMainServer(ticker)
	select {}
}
