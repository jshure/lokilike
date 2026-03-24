package ingester

import (
	"context"
	"log/slog"
	"time"

	"github.com/joel-shure/sigyn/internal/domain"
	"github.com/joel-shure/sigyn/internal/index"
	"github.com/joel-shure/sigyn/internal/retry"
	"github.com/joel-shure/sigyn/internal/storage"
)

// S3Flusher implements the Flusher interface by writing chunks to S3 with retries
// and writing a label index file alongside each chunk.
type S3Flusher struct {
	client     *storage.S3Client
	indexStore *index.Store // nil = no index
}

// NewS3Flusher creates a flusher backed by S3.
// If indexStore is non-nil, a label index entry is written for each chunk.
func NewS3Flusher(client *storage.S3Client, indexStore *index.Store) *S3Flusher {
	return &S3Flusher{client: client, indexStore: indexStore}
}

// Flush writes the compressed chunk data to S3 with exponential-backoff retries,
// then writes the label index file.
func (f *S3Flusher) Flush(chunk domain.Chunk, compressed []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := retry.Do(ctx, retry.DefaultOptions, func() error {
		_, err := f.client.PutObject(ctx, chunk.Key, compressed)
		if err != nil {
			slog.Warn("s3 flush attempt failed, retrying", "key", chunk.Key, "error", err)
		}
		return err
	})
	if err != nil {
		return err
	}

	// Write label index (best-effort — chunk is already persisted).
	if f.indexStore != nil {
		idx := index.ChunkIndex{
			ChunkKey:    chunk.Key,
			Service:     chunk.Service,
			MinTime:     chunk.MinTime,
			MaxTime:     chunk.MaxTime,
			LabelSets:   chunk.LabelSets,
			EntryCount:  chunk.EntryCount,
			SizeBytes:   chunk.SizeBytes,
			Compression: string(chunk.Compression),
		}
		if err := f.indexStore.Write(ctx, idx); err != nil {
			slog.Error("index write failed", "chunk", chunk.Key, "error", err)
			// Non-fatal: the chunk is safe in S3, the index can be rebuilt.
		}
	}

	return nil
}
