package mainserver

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	persistent_hashmap "tatzelwurm/utils"
	"time"

	"github.com/google/uuid"
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
	chunk_id      string
	is_replicated bool
}

func (c *ChunkIdToChunkServerPersistentHashmap) Get(key string) (ChunkIdToChunkServerPersistentHashmapValue, bool) {
	value, ok := c.PersistentHashmap.Get(key)
	values := strings.Split(value, ",")
	var return_value ChunkIdToChunkServerPersistentHashmapValue
	if ok {
		is_replicated_value, err := strconv.ParseBool(values[1])
		if err != nil {
			return return_value, false
		}
		return_value = ChunkIdToChunkServerPersistentHashmapValue{
			chunk_id:      values[0],
			is_replicated: is_replicated_value,
		}
	}
	return return_value, ok
}

func (c *ChunkIdToChunkServerPersistentHashmap) Put(key string, value ChunkIdToChunkServerPersistentHashmapValue) bool {
	value_string := fmt.Sprintf("%s, %v", value.chunk_id, value.is_replicated)
	return c.PersistentHashmap.Put(key, value_string)
}

type ChunkServerPersistentHashmap struct {
	*MainServerPersistentHashmap
}

type ChunkServerPersistentHashmapValue struct {
	ipv4_address string
}

func (c *ChunkServerPersistentHashmap) Get(key string) (ChunkServerPersistentHashmapValue, bool) {
	value, ok := c.PersistentHashmap.Get(key)
	return_value := ChunkServerPersistentHashmapValue{
		ipv4_address: value,
	}
	return return_value, ok
}
func (c *ChunkServerPersistentHashmap) Put(key string, value ChunkServerPersistentHashmapValue) bool {
	value_string := fmt.Sprintf("%s", value.ipv4_address)
	return c.PersistentHashmap.Put(key, value_string)
}

type FileNameToChunkIdPersistentHashmap struct {
	*MainServerPersistentHashmap
}

type FileNameToChunkIdPersistentHashmapValue struct {
	chunk_id string
}

func (f *FileNameToChunkIdPersistentHashmap) Get(key string) (FileNameToChunkIdPersistentHashmapValue, bool) {
	value, ok := f.PersistentHashmap.Get(key)
	return_value := FileNameToChunkIdPersistentHashmapValue{
		chunk_id: value,
	}
	return return_value, ok
}

func (f *FileNameToChunkIdPersistentHashmap) Put(key string, value FileNameToChunkIdPersistentHashmapValue) bool {
	value_string := fmt.Sprintf("%s", value.chunk_id)
	return f.PersistentHashmap.Put(key, value_string)
}

var mainServerConfigFileJson MAIN_SERVER_CONFIG
var chunk_id_to_chunk_server_maps map[string]ChunkIdToChunkServerPersistentHashmap = make(map[string]ChunkIdToChunkServerPersistentHashmap)
var chunk_server_map *ChunkServerPersistentHashmap = nil
var file_name_to_chunk_id_map *FileNameToChunkIdPersistentHashmap = nil

func processBase(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("This is the gfs mainserver..."))
}

func processGetChunkId(w http.ResponseWriter, r *http.Request) {
	var response map[string]string = nil
	if r.Method == http.MethodGet {
		file_name := r.URL.Query().Get("file_name")
		chunk_offset := r.URL.Query().Get("chunk_offset")
		file_name_to_chunk_id_map_value, chunk_id_found := file_name_to_chunk_id_map.Get(fmt.Sprintf("%s, %s", file_name, chunk_offset))
		fmt.Printf("The file_name is %s, and the chunk_offset is %s\n", file_name, chunk_offset)
		if chunk_id_found {
			w.WriteHeader(http.StatusConflict)
			response = map[string]string{
				"description": "chunk_id found successfully",
				"chunk_id":    file_name_to_chunk_id_map_value.chunk_id,
			}
		} else {
			w.WriteHeader(http.StatusNotFound)
			response = map[string]string{
				"description": "chunk_id not found in store!",
				"chunk_id":    "",
			}
		}
	} else if r.Method == http.MethodPost {
		var request_body_json map[string]string
		json.NewDecoder(r.Body).Decode(&request_body_json)
		file_name := request_body_json["file_name"]
		chunk_offset, _ := strconv.Atoi(request_body_json["chunk_offset"])
		println("filename: ", file_name)
		println("chunk_offset: ", chunk_offset)
		file_name_to_chunk_id_map_value, chunk_id_found := file_name_to_chunk_id_map.Get(fmt.Sprintf("%s, %d", file_name, chunk_offset))
		fmt.Printf("The file_name is %s, and the chunk_offset is %d\n", file_name, chunk_offset)
		if chunk_id_found {
			w.WriteHeader(http.StatusConflict)
			response = map[string]string{
				"description": "chunk_id already exists!",
				"chunk_id":    file_name_to_chunk_id_map_value.chunk_id,
			}
		} else {

			chunk_id := uuid.New().String()
			file_name_to_chunk_id_map.Put(fmt.Sprintf("%s, %d", file_name, chunk_offset), FileNameToChunkIdPersistentHashmapValue{
				chunk_id: chunk_id,
			})
			response = map[string]string{
				"description": "chunk_id not found in store! Returning a fresh one...",
				"chunk_id":    chunk_id,
			}
		}
	} else {
		response = map[string]string{
			"description": "Unsupported method...",
		}
	}
	json.NewEncoder(w).Encode(response)
}

func getRandomKeys(m *MainServerPersistentHashmap, n int) ([]string, string) {
	if len(m.HashMap) < n {
		return nil, "The amount to sample is more than the number of keys"
	}
	keys := make([]string, 0, len(m.HashMap))
	for k := range m.HashMap {
		keys = append(keys, k)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(m.HashMap), func(i int, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys[:n], ""
}

func processGetChunkServers(w http.ResponseWriter, r *http.Request) {
	type GetChunkServerModel struct {
		Session_id   string `json:"session_id"`
		Ipv4_address string `json:"ipv4_address"`
	}
	type GetChunkServersResponse struct {
		Description   string                `json:"description"`
		Chunk_servers []GetChunkServerModel `json:"chunk_servers"`
	}
	var response GetChunkServersResponse
	if r.Method == http.MethodGet {
		chunk_id := r.URL.Query().Get("chunk_id")
		fmt.Printf("The chunk_id is %s\n", chunk_id)
		chunk_servers := make([]GetChunkServerModel, 0)
		for _, chunk_id_to_chunk_server_map := range chunk_id_to_chunk_server_maps {
			chunk_id_to_chunk_server_map_value, ok := chunk_id_to_chunk_server_map.Get(chunk_id)
			chunk_id_to_chunk_server_map_values := strings.Split(chunk_id_to_chunk_server_map_value.chunk_id, ",")
			chunkserver_session_id := chunk_id_to_chunk_server_map_values[0]
			// chunkid_is_replicated := chunk_id_to_chunk_server_map_values[1]
			if ok {
				chunk_server_map_value, _ := chunk_server_map.Get(chunkserver_session_id)
				// chunkserver_is_replicated := chunkserver_map_values[1]
				chunk_servers = append(chunk_servers, GetChunkServerModel{
					Session_id:   chunkserver_session_id,
					Ipv4_address: chunk_server_map_value.ipv4_address,
				})
			}
		}
		response = GetChunkServersResponse{
			Description:   fmt.Sprintf("Listing the chunk servers available for the chunk_id %s", chunk_id),
			Chunk_servers: chunk_servers,
		}

	} else if r.Method == http.MethodPost {
		// var chunk_servers []GetChunkServerModel
		var request_body_json map[string]string
		json.NewDecoder(r.Body).Decode(&request_body_json)
		chunk_id := request_body_json["chunk_id"]
		print("Chunk id is", chunk_id)
		// TODO: check if chunk_id already exists or not

		chunk_server_keys, err := getRandomKeys(chunk_server_map.MainServerPersistentHashmap, mainServerConfigFileJson.REPLICATION_FACTOR)
		if err == "" {
			print(chunk_server_keys)
			// fetch the chunk_servers
			// var chunk_servers []GetChunkServerModel
			// chunk_servers = append(chunk_servers, GetChunkServerModel{
			// 	session_id: chunk,
			// })
			chunk_servers := make([]GetChunkServerModel, 0)
			for idx, chunk_server_key := range chunk_server_keys {
				// We are sure to find chunk_server_key in chunk_server_map
				chunk_server_map_value, _ := chunk_server_map.Get(chunk_server_key)
				map_name := fmt.Sprintf("persistent_hashmap_%d", idx)
				chunk_id_to_chunk_server_map := chunk_id_to_chunk_server_maps[map_name]
				chunk_id_to_chunk_server_map.Put(chunk_id, ChunkIdToChunkServerPersistentHashmapValue{
					chunk_id:      chunk_server_key,
					is_replicated: false,
				})
				chunk_servers = append(chunk_servers, GetChunkServerModel{
					Session_id:   chunk_server_key,
					Ipv4_address: chunk_server_map_value.ipv4_address,
				})
			}
			response = GetChunkServersResponse{
				Description:   fmt.Sprintf("Allocated chunkservers to the chunk_id %s", chunk_id),
				Chunk_servers: chunk_servers,
			}
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			response = GetChunkServersResponse{
				Description:   "Error occured while fetching chunk_servers",
				Chunk_servers: make([]GetChunkServerModel, 0),
			}
		}
	} else {

	}
	json.NewEncoder(w).Encode(response)
}

func processHeartBeat(w http.ResponseWriter, r *http.Request) {
	var response map[string]string
	if r.Method == http.MethodGet {
		response = map[string]string{
			"message":    "The gfs mainserver is alive and healthy...",
			"chunk_size": strconv.Itoa(mainServerConfigFileJson.GFS_CHUNK_SIZE),
		}
	} else if r.Method == http.MethodPost {
		var request_body *PROCESS_HEARTBEAT_POST_REQUEST_BODY
		json.NewDecoder(r.Body).Decode(&request_body)
		fmt.Printf("[heartbeat_request] Recieved request with ip_address->%s\n", request_body.IP_ADDRESS)
		chunk_server_session_id, entry_found := chunk_server_map.Check_value(request_body.IP_ADDRESS)
		if !entry_found {
			// this chunk server is not registered with the mainserver
			chunk_server_session_id := uuid.New().String()
			chunk_server_map.Put(chunk_server_session_id, ChunkServerPersistentHashmapValue{
				ipv4_address: request_body.IP_ADDRESS,
			})
			response = map[string]string{
				"description": fmt.Sprintf("[heartbeat_request] This chunkserver is successfully registered with the mainserver with session id %s", chunk_server_session_id),
			}
		} else {
			response = map[string]string{
				"description": fmt.Sprintf("[heartbeat_request] This chunkserver is already registered with the mainserver with session id %s", chunk_server_session_id),
			}
			fmt.Println(response["description"])
		}
	} else {
		response = map[string]string{
			"description": fmt.Sprintf("Method %s not supported", r.Method),
		}
	}
	json.NewEncoder(w).Encode(response)
}

func Run(mainserver_config_file_path string) {
	fmt.Println("Running as the mainserver...")
	mainServerConfigFileData, err := os.ReadFile(mainserver_config_file_path)
	if err != nil {
		fmt.Println("Error opening file at path ", mainserver_config_file_path)
	}
	err1 := json.Unmarshal(mainServerConfigFileData, &mainServerConfigFileJson)
	if err1 != nil {
		fmt.Println("Error parsing the json in the file at path ", mainserver_config_file_path)
	}
	// Data structures used by mainserver
	// var chunk_id_to_chunk_server_map
	for i := 0; i < mainServerConfigFileJson.REPLICATION_FACTOR; i++ {
		map_name := fmt.Sprintf("persistent_hashmap_%d", i)
		chunk_id_to_chunk_server_maps[map_name] = ChunkIdToChunkServerPersistentHashmap{
			MainServerPersistentHashmap: &MainServerPersistentHashmap{
				Config: mainServerConfigFileJson,
				PersistentHashmap: &persistent_hashmap.PersistentHashmap{
					AuditLogFilePath: filepath.Join(mainServerConfigFileJson.PERSISTENT_HASHMAP_WAL_DIRECTORY, fmt.Sprintf("%s_audit.log", map_name)),
					Namespace:        map_name,
				},
			},
		}
		chunk_id_to_chunk_server_maps[map_name].PersistentHashmap.Initialize()
	}

	chunk_server_map = &ChunkServerPersistentHashmap{
		MainServerPersistentHashmap: &MainServerPersistentHashmap{
			Config: mainServerConfigFileJson,
			PersistentHashmap: &persistent_hashmap.PersistentHashmap{
				AuditLogFilePath: filepath.Join(mainServerConfigFileJson.PERSISTENT_HASHMAP_WAL_DIRECTORY, "chunk_server_map_audit.log"),
				Namespace:        "chunk_server_map",
			},
		},
	}
	chunk_server_map.PersistentHashmap.Initialize()
	file_name_to_chunk_id_map = &FileNameToChunkIdPersistentHashmap{
		MainServerPersistentHashmap: &MainServerPersistentHashmap{
			Config: mainServerConfigFileJson,
			PersistentHashmap: &persistent_hashmap.PersistentHashmap{
				AuditLogFilePath: filepath.Join(mainServerConfigFileJson.PERSISTENT_HASHMAP_WAL_DIRECTORY, "file_name_to_chunk_id_map_audit.log"),
				Namespace:        "file_name_to_chunk_id_map",
			},
		},
	}
	file_name_to_chunk_id_map.PersistentHashmap.Initialize()

	http.HandleFunc("/", processBase)
	http.HandleFunc("/heartbeat", processHeartBeat)
	http.HandleFunc("/get_chunk_servers", processGetChunkServers)
	http.HandleFunc("/get_chunk_id", processGetChunkId)

	var portNumber = mainServerConfigFileJson.PORT
	if err := http.ListenAndServe("localhost:"+strconv.Itoa(portNumber), nil); err != nil {
		fmt.Println("Could not start http server on port ", portNumber, err)
	}
}
