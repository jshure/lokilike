package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	appconfig "github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/metrics"
)

// S3Client wraps the AWS S3 SDK with our bucket/prefix defaults.
type S3Client struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Client builds an S3Client from our app config.
func NewS3Client(ctx context.Context, cfg appconfig.S3Config) (*S3Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		slog.Debug("s3: custom endpoint", "endpoint", cfg.Endpoint, "path_style", cfg.UsePathStyle)
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.UsePathStyle
		})
	}

	slog.Debug("s3: initialized", "bucket", cfg.Bucket, "prefix", cfg.Prefix, "region", cfg.Region)

	return &S3Client{
		client: s3.NewFromConfig(awsCfg, s3Opts...),
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// PutObject writes data to s3://<bucket>/<prefix><key>.
func (c *S3Client) PutObject(ctx context.Context, key string, data []byte) (int64, error) {
	fullKey := c.prefix + key
	start := time.Now()
	slog.Debug("s3 put", "key", fullKey, "bytes", len(data))

	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
		Body:   bytes.NewReader(data),
	})

	dur := time.Since(start)
	metrics.S3Duration.WithLabelValues("PutObject").Observe(dur.Seconds())
	if err != nil {
		metrics.S3Operations.WithLabelValues("PutObject", "error").Inc()
		return 0, fmt.Errorf("s3 put %s: %w", fullKey, err)
	}
	metrics.S3Operations.WithLabelValues("PutObject", "ok").Inc()
	return int64(len(data)), nil
}

// GetObject fetches the object at s3://<bucket>/<prefix><key>.
func (c *S3Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	fullKey := c.prefix + key
	return c.getObject(ctx, fullKey)
}

// GetObjectRaw fetches an object by its full S3 key (no prefix prepended).
func (c *S3Client) GetObjectRaw(ctx context.Context, fullKey string) ([]byte, error) {
	return c.getObject(ctx, fullKey)
}

func (c *S3Client) getObject(ctx context.Context, fullKey string) ([]byte, error) {
	start := time.Now()
	slog.Debug("s3 get", "key", fullKey)

	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
	})

	dur := time.Since(start)
	metrics.S3Duration.WithLabelValues("GetObject").Observe(dur.Seconds())
	if err != nil {
		metrics.S3Operations.WithLabelValues("GetObject", "error").Inc()
		return nil, fmt.Errorf("s3 get %s: %w", fullKey, err)
	}
	metrics.S3Operations.WithLabelValues("GetObject", "ok").Inc()
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// ListObjects returns all keys under s3://<bucket>/<prefix><keyPrefix>.
func (c *S3Client) ListObjects(ctx context.Context, keyPrefix string) ([]string, error) {
	fullPrefix := c.prefix + keyPrefix
	start := time.Now()
	slog.Debug("s3 list", "prefix", fullPrefix)

	var keys []string

	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			metrics.S3Operations.WithLabelValues("ListObjects", "error").Inc()
			return nil, fmt.Errorf("s3 list %s: %w", fullPrefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}

	dur := time.Since(start)
	metrics.S3Duration.WithLabelValues("ListObjects").Observe(dur.Seconds())
	metrics.S3Operations.WithLabelValues("ListObjects", "ok").Inc()
	slog.Debug("s3 list done", "prefix", fullPrefix, "keys", len(keys))
	return keys, nil
}
