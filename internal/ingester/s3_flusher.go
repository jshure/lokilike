package ingester

import (
	"context"
	"log/slog"
	"time"

	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/retry"
	"github.com/joel-shure/lokilike/internal/storage"
)

// S3Flusher implements the Flusher interface by writing chunks to S3 with retries.
type S3Flusher struct {
	client *storage.S3Client
}

// NewS3Flusher creates a flusher backed by S3.
func NewS3Flusher(client *storage.S3Client) *S3Flusher {
	return &S3Flusher{client: client}
}

// Flush writes the compressed chunk data to S3 with exponential-backoff retries.
func (f *S3Flusher) Flush(chunk domain.Chunk, compressed []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	return retry.Do(ctx, retry.DefaultOptions, func() error {
		_, err := f.client.PutObject(ctx, chunk.Key, compressed)
		if err != nil {
			slog.Warn("s3 flush attempt failed, retrying", "key", chunk.Key, "error", err)
		}
		return err
	})
}
