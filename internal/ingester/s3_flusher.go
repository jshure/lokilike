package ingester

import (
	"context"

	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/logger"
	"github.com/joel-shure/lokilike/internal/storage"
)

// S3Flusher implements the Flusher interface by writing chunks to S3.
type S3Flusher struct {
	client *storage.S3Client
}

// NewS3Flusher creates a flusher backed by S3.
func NewS3Flusher(client *storage.S3Client) *S3Flusher {
	return &S3Flusher{client: client}
}

// Flush writes the compressed chunk data to S3.
func (f *S3Flusher) Flush(chunk domain.Chunk, compressed []byte) error {
	logger.Get().Debug("s3 flusher: writing %d bytes to key %s", len(compressed), chunk.Key)
	_, err := f.client.PutObject(context.Background(), chunk.Key, compressed)
	return err
}
