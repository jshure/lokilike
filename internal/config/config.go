package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Debug    bool           `json:"debug"`
	Ingester IngesterConfig `json:"ingester"`
	Storage  StorageConfig  `json:"storage"`
	Exporter ExporterConfig `json:"exporter"`
}

type IngesterConfig struct {
	ListenAddress      string `json:"listen_address"`
	BatchSizeBytes     int    `json:"batch_size_bytes"`
	BatchTimeWindowSec int    `json:"batch_time_window_sec"`
	CompressionAlgo    string `json:"compression_algo"`
}

type StorageConfig struct {
	S3 S3Config `json:"s3"`
}

type S3Config struct {
	Bucket       string `json:"bucket"`
	Region       string `json:"region"`
	Prefix       string `json:"prefix"`
	Endpoint     string `json:"endpoint"`
	UsePathStyle bool   `json:"use_path_style"`
}

type ExporterConfig struct {
	OpenSearch       OpenSearchConfig `json:"opensearch"`
	DefaultBatchSize int              `json:"default_batch_size"`
}

type OpenSearchConfig struct {
	Endpoint    string `json:"endpoint"`
	IndexPrefix string `json:"index_prefix"`
	Username    string `json:"username"`
	Password    string `json:"password"`
}

// Load reads a JSON config file and expands environment variables in
// string fields that start with "$".
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
	return &cfg, nil
}
