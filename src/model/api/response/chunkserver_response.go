package response_dto

type GetChunkResponseModel struct {
	Description  string `json:description`
	ChunkContent string `json:chunk_content`
}
type WriteChunkResponseModel struct {
	Description string `json:"description"`
}
type SyncWithMainserverResponseModel struct {
	Chunk_id string `json:"chunk_id"`
	Status   string `json:"status"`
}
