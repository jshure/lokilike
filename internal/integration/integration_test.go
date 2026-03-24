//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/ingester"
	"github.com/joel-shure/lokilike/internal/storage"
)

func minioEndpoint() string {
	if e := os.Getenv("MINIO_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:9000"
}

func skipIfNoMinIO(t *testing.T) {
	t.Helper()
	endpoint := minioEndpoint()
	resp, err := http.Get(endpoint + "/minio/health/live")
	if err != nil || resp.StatusCode != 200 {
		t.Skipf("MinIO not available at %s", endpoint)
	}
}

func newTestS3Client(t *testing.T, prefix string) *storage.S3Client {
	t.Helper()
	ctx := context.Background()
	client, err := storage.NewS3Client(ctx, config.S3Config{
		Bucket:       "lokilike-dev",
		Region:       "us-east-1",
		Prefix:       prefix,
		Endpoint:     minioEndpoint(),
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("create S3 client: %v", err)
	}
	return client
}

func TestIntegration_PutAndGetObject(t *testing.T) {
	skipIfNoMinIO(t)
	prefix := "test/" + uuid.NewString() + "/"
	client := newTestS3Client(t, prefix)
	ctx := context.Background()

	payload := []byte("hello from integration test")
	key := "testfile.txt"

	_, err := client.PutObject(ctx, key, payload)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got, err := client.GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("data mismatch: got %q, want %q", got, payload)
	}
}

func TestIntegration_ListObjects(t *testing.T) {
	skipIfNoMinIO(t)
	prefix := "test/" + uuid.NewString() + "/"
	client := newTestS3Client(t, prefix)
	ctx := context.Background()

	keys := []string{"a/1.txt", "a/2.txt", "b/3.txt"}
	for _, k := range keys {
		if _, err := client.PutObject(ctx, k, []byte("data")); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}

	all, err := client.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(all), all)
	}

	aOnly, err := client.ListObjects(ctx, "a/")
	if err != nil {
		t.Fatalf("ListObjects a/: %v", err)
	}
	if len(aOnly) != 2 {
		t.Fatalf("expected 2 keys under a/, got %d: %v", len(aOnly), aOnly)
	}
}

func TestIntegration_IngestAndReadBack(t *testing.T) {
	skipIfNoMinIO(t)
	prefix := "test/" + uuid.NewString() + "/"
	client := newTestS3Client(t, prefix)
	ctx := context.Background()

	flusher := ingester.NewS3Flusher(client)
	buf := ingester.NewBuffer(ingester.BufferOpts{
		MaxBytes:    512,
		MaxAge:      5 * time.Second,
		Flusher:     flusher,
		Compression: domain.CompressionGzip,
	})

	now := time.Now().UTC()
	entries := []domain.LogEntry{
		{Timestamp: now, Service: "testsvc", Level: "info", Message: "line 1"},
		{Timestamp: now.Add(time.Second), Service: "testsvc", Level: "warn", Message: "line 2"},
		{Timestamp: now.Add(2 * time.Second), Service: "testsvc", Level: "error", Message: "line 3"},
	}
	for _, e := range entries {
		buf.Add(e)
	}
	buf.Stop()

	keys, err := client.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("expected at least one chunk in S3")
	}

	var allEntries []domain.LogEntry
	for _, key := range keys {
		data, err := client.GetObjectRaw(ctx, key)
		if err != nil {
			t.Fatalf("GetObjectRaw %s: %v", key, err)
		}
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("gzip reader %s: %v", key, err)
		}
		scanner := bufio.NewScanner(gz)
		for scanner.Scan() {
			var e domain.LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			allEntries = append(allEntries, e)
		}
		gz.Close()
	}

	if len(allEntries) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(allEntries))
	}

	messages := map[string]bool{}
	for _, e := range allEntries {
		messages[e.Message] = true
	}
	for _, e := range entries {
		if !messages[e.Message] {
			t.Errorf("missing message: %s", e.Message)
		}
	}
}
