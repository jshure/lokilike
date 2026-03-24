package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Debug    bool           `json:"debug"`
	TenantID string        `json:"tenant_id"`
	Ingester IngesterConfig `json:"ingester"`
	Storage  StorageConfig  `json:"storage"`
	Exporter ExporterConfig `json:"exporter"`
	Auth     AuthConfig     `json:"auth"`
	Metrics  MetricsConfig  `json:"metrics"`
}

type IngesterConfig struct {
	ListenAddress      string    `json:"listen_address"`
	BatchSizeBytes     int       `json:"batch_size_bytes"`
	BatchTimeWindowSec int       `json:"batch_time_window_sec"`
	CompressionAlgo    string    `json:"compression_algo"`
	MaxBodyBytes       int64     `json:"max_body_bytes"`
	MaxEntriesPerReq   int       `json:"max_entries_per_request"`
	RateLimitRPS       float64   `json:"rate_limit_rps"`
	WALDir             string    `json:"wal_dir"`
	TLS                TLSConfig `json:"tls"`
	MinLevel           string    `json:"min_level"`
}

type TLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

type StorageConfig struct {
	S3    S3Config    `json:"s3"`
	Index IndexConfig `json:"index"`
}

type S3Config struct {
	Bucket        string `json:"bucket"`
	Region        string `json:"region"`
	Prefix        string `json:"prefix"`
	Endpoint      string `json:"endpoint"`
	AccessKey     string `json:"access_key"`
	SecretKey     string `json:"secret_key"`
	UsePathStyle  bool   `json:"use_path_style"`
	RetentionDays int    `json:"retention_days"`
}

type ExporterConfig struct {
	ListenAddress     string           `json:"listen_address"`
	OpenSearch        OpenSearchConfig `json:"opensearch"`
	DefaultBatchSize  int              `json:"default_batch_size"`
	MaxConcurrentJobs int              `json:"max_concurrent_jobs"`
	IngesterURL       string           `json:"ingester_url"`
}

type IndexConfig struct {
	Prefix string `json:"prefix"`
}

type OpenSearchConfig struct {
	Endpoint    string `json:"endpoint"`
	IndexPrefix string `json:"index_prefix"`
	Username    string `json:"username"`
	Password    string `json:"password"`
}

type AuthConfig struct {
	Enabled bool     `json:"enabled"`
	APIKeys []string `json:"api_keys"`
}

type MetricsConfig struct {
	Enabled bool   `json:"enabled"`
	Address string `json:"address"`
}

// Load reads a JSON config file and expands environment variables.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data = []byte(os.ExpandEnv(string(data)))

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Ingester.ListenAddress == "" {
		c.Ingester.ListenAddress = ":8082"
	}
	if c.Ingester.BatchSizeBytes <= 0 {
		c.Ingester.BatchSizeBytes = 5 * 1024 * 1024
	}
	if c.Ingester.BatchTimeWindowSec <= 0 {
		c.Ingester.BatchTimeWindowSec = 30
	}
	if c.Ingester.CompressionAlgo == "" {
		c.Ingester.CompressionAlgo = "gzip"
	}
	if c.Ingester.MaxBodyBytes <= 0 {
		c.Ingester.MaxBodyBytes = 10 * 1024 * 1024 // 10 MB
	}
	if c.Ingester.MaxEntriesPerReq <= 0 {
		c.Ingester.MaxEntriesPerReq = 10000
	}
	if c.Exporter.ListenAddress == "" {
		c.Exporter.ListenAddress = ":8081"
	}
	if c.Exporter.DefaultBatchSize <= 0 {
		c.Exporter.DefaultBatchSize = 1000
	}
	if c.Exporter.MaxConcurrentJobs <= 0 {
		c.Exporter.MaxConcurrentJobs = 4
	}
	if c.Exporter.OpenSearch.IndexPrefix == "" {
		c.Exporter.OpenSearch.IndexPrefix = "exported-logs-"
	}
	if c.Metrics.Address == "" {
		c.Metrics.Address = ":9090"
	}
	if c.Storage.Index.Prefix == "" {
		c.Storage.Index.Prefix = "index/"
	}
	if c.Exporter.IngesterURL == "" {
		c.Exporter.IngesterURL = "ws://localhost:8082"
	}
}

// Validate checks that required fields are present and values are sane.
func (c *Config) Validate() error {
	var errs []string

	if c.Storage.S3.Bucket == "" {
		errs = append(errs, "storage.s3.bucket is required")
	}
	if c.Storage.S3.Region == "" {
		errs = append(errs, "storage.s3.region is required")
	}
	if c.Exporter.OpenSearch.Endpoint == "" {
		errs = append(errs, "exporter.opensearch.endpoint is required")
	}

	algo := c.Ingester.CompressionAlgo
	if algo != "gzip" && algo != "snappy" {
		errs = append(errs, fmt.Sprintf("ingester.compression_algo must be gzip or snappy, got %q", algo))
	}

	if c.Ingester.TLS.Enabled {
		if c.Ingester.TLS.CertFile == "" || c.Ingester.TLS.KeyFile == "" {
			errs = append(errs, "ingester.tls.cert_file and key_file required when tls.enabled is true")
		}
	}

	if c.Auth.Enabled && len(c.Auth.APIKeys) == 0 {
		errs = append(errs, "auth.api_keys must not be empty when auth.enabled is true")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed: %s", strings.Join(errs, "; "))
	}
	return nil
}
