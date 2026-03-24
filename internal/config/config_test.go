package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"storage": {"s3": {"bucket": "b", "region": "us-east-1"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.S3.Bucket != "b" {
		t.Errorf("bucket = %q, want %q", cfg.Storage.S3.Bucket, "b")
	}
}

func TestLoad_AppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"storage": {"s3": {"bucket": "b", "region": "us-east-1"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Ingester.ListenAddress != ":8082" {
		t.Errorf("listen_address = %q, want :8082", cfg.Ingester.ListenAddress)
	}
	if cfg.Ingester.BatchSizeBytes != 5*1024*1024 {
		t.Errorf("batch_size = %d, want %d", cfg.Ingester.BatchSizeBytes, 5*1024*1024)
	}
	if cfg.Ingester.MaxBodyBytes != 10*1024*1024 {
		t.Errorf("max_body = %d, want %d", cfg.Ingester.MaxBodyBytes, 10*1024*1024)
	}
	if cfg.Exporter.MaxConcurrentJobs != 4 {
		t.Errorf("max_concurrent = %d, want 4", cfg.Exporter.MaxConcurrentJobs)
	}
	if cfg.Storage.Index.Prefix != "index/" {
		t.Errorf("index_prefix = %q, want index/", cfg.Storage.Index.Prefix)
	}
}

func TestValidate_MissingBucket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"storage": {"s3": {"bucket": "", "region": "us-east-1"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for empty bucket")
	}
}

func TestValidate_MissingRegion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"storage": {"s3": {"bucket": "b", "region": ""}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for empty region")
	}
}

func TestValidate_InvalidCompression(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"ingester": {"compression_algo": "zstd"},
		"storage": {"s3": {"bucket": "b", "region": "r"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for unsupported compression")
	}
}

func TestValidate_TLSWithoutCert(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"ingester": {"tls": {"enabled": true}},
		"storage": {"s3": {"bucket": "b", "region": "r"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for TLS without cert/key")
	}
}

func TestValidate_AuthWithoutKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"auth": {"enabled": true, "api_keys": []},
		"storage": {"s3": {"bucket": "b", "region": "r"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for auth without keys")
	}
}

func TestLoad_EnvExpansion(t *testing.T) {
	t.Setenv("TEST_BUCKET", "my-bucket")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"storage": {"s3": {"bucket": "${TEST_BUCKET}", "region": "us-east-1"}},
		"exporter": {"opensearch": {"endpoint": "http://localhost:9200"}}
	}`), 0o644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.S3.Bucket != "my-bucket" {
		t.Errorf("bucket = %q, want my-bucket", cfg.Storage.S3.Bucket)
	}
}
