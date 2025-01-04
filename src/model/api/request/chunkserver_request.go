package request_dto

type SyncWithMainserverRequestChunkListModel = SyncFromChunkServerRequestChunkListModel

type SyncWithMainserverRequestModel struct {
	Chunk_list   []SyncWithMainserverRequestChunkListModel `json:"chunk_list"`
	Ipv4_address string                                    `json:"ipv4_address"`
}
