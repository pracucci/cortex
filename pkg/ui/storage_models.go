package ui

import (
	"time"

	"github.com/oklog/ulid"

	"github.com/cortexproject/cortex/pkg/querier"
)

type Block struct {
	BlockID    ulid.ULID       `json:"block_id"`
	MinTime    int64           `json:"min_time"`
	MaxTime    int64           `json:"max_time"`
	UploadedAt time.Time       `json:"uploaded_at"`
	Compaction BlockCompaction `json:"compaction"`
	Stats      BlockStats      `json:"stats"`
}

type BlockCompaction struct {
	Level int `json:"level"`
}

type BlockStats struct {
	NumSamples    uint64 `json:"num_samples"`
	NumSeries     uint64 `json:"num_series"`
	NumChunks     uint64 `json:"num_chunks"`
	NumTombstones uint64 `json:"num_tombstones"`

	// Extended statistics (available only when requesting a specific block).
	SizeIndexBytes  uint64 `json:"size_index_bytes"`
	SizeChunksBytes uint64 `json:"size_chunks_bytes"`
	SizeMetaBytes   uint64 `json:"size_meta_bytes"`
}

func createBlockFromMeta(meta *querier.BlockMeta) Block {
	return Block{
		BlockID:    meta.ULID,
		MinTime:    meta.MinTime,
		MaxTime:    meta.MaxTime,
		UploadedAt: meta.UploadedAt,
		Compaction: BlockCompaction{
			Level: meta.Compaction.Level,
		},
		Stats: BlockStats{
			NumSamples:    meta.Stats.NumSamples,
			NumSeries:     meta.Stats.NumSeries,
			NumChunks:     meta.Stats.NumChunks,
			NumTombstones: meta.Stats.NumTombstones,
		},
	}
}
