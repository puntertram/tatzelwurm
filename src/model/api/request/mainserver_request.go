package request_dto

type SyncFromChunkServerRequestChunkListModel struct {
	Chunk_id      string `json:"chunk_id"`
	Is_replicated bool   `json:"is_replicated"`
}
type SyncFromChunkServerRequestBody struct {
	Chunk_list   []SyncFromChunkServerRequestChunkListModel `json:"chunk_list"`
	Ipv4_address string                                     `json:"ipv4_address"`
}
