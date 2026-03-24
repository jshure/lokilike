package exporter

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/domain"
	"github.com/joel-shure/lokilike/internal/logger"
)

// OSClient wraps the OpenSearch Go client for bulk indexing.
type OSClient struct {
	client      *opensearch.Client
	indexPrefix string
}

// NewOSClient creates an OpenSearch client from config.
func NewOSClient(cfg config.OpenSearchConfig) (*OSClient, error) {
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{cfg.Endpoint},
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("opensearch client: %w", err)
	}
	return &OSClient{client: client, indexPrefix: cfg.IndexPrefix}, nil
}

// BulkIndex sends a batch of log entries to OpenSearch using the Bulk API.
// The index name is derived from the entry timestamp: <prefix>YYYY.MM.DD
func (c *OSClient) BulkIndex(ctx context.Context, entries []domain.LogEntry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}

	log := logger.Get()
	log.Debug("opensearch: bulk indexing %d entries", len(entries))

	var buf bytes.Buffer
	for _, entry := range entries {
		index := c.indexPrefix + entry.Timestamp.UTC().Format("2006.01.02")

		meta := map[string]any{
			"index": map[string]any{
				"_index": index,
			},
		}
		metaLine, _ := json.Marshal(meta)
		docLine, err := json.Marshal(entry)
		if err != nil {
			return 0, fmt.Errorf("marshal entry: %w", err)
		}

		buf.Write(metaLine)
		buf.WriteByte('\n')
		buf.Write(docLine)
		buf.WriteByte('\n')
	}

	log.Debug("opensearch: bulk payload %d bytes", buf.Len())

	res, err := c.client.Bulk(
		strings.NewReader(buf.String()),
		c.client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk request: %w", err)
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.IsError() {
		return 0, fmt.Errorf("bulk response %s: %s", res.Status(), truncate(string(body), 500))
	}

	var bulkResp struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				Status int `json:"status"`
				Error  any `json:"error"`
			} `json:"index"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &bulkResp); err != nil {
		return 0, fmt.Errorf("parse bulk response: %w", err)
	}

	indexed := 0
	for _, item := range bulkResp.Items {
		if item.Index.Status >= 200 && item.Index.Status < 300 {
			indexed++
		}
	}

	if bulkResp.Errors {
		log.Error("opensearch: bulk response contained errors (%d/%d succeeded)", indexed, len(entries))
	}

	return indexed, nil
}

// Ping checks connectivity to the OpenSearch cluster.
func (c *OSClient) Ping(ctx context.Context) error {
	res, err := c.client.Info(
		c.client.Info.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("opensearch ping: %s", res.Status())
	}
	return nil
}

// IndexNameForDate returns the index name for a given date.
func (c *OSClient) IndexNameForDate(t time.Time) string {
	return c.indexPrefix + t.UTC().Format("2006.01.02")
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
