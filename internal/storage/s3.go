package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	appconfig "github.com/joel-shure/lokilike/internal/config"
	"github.com/joel-shure/lokilike/internal/logger"
)

// S3Client wraps the AWS S3 SDK with our bucket/prefix defaults.
type S3Client struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Client builds an S3Client from our app config.
func NewS3Client(ctx context.Context, cfg appconfig.S3Config) (*S3Client, error) {
	log := logger.Get()

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		log.Debug("s3: using custom endpoint %s (path_style=%v)", cfg.Endpoint, cfg.UsePathStyle)
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.UsePathStyle
		})
	}

	log.Debug("s3: bucket=%s prefix=%s region=%s", cfg.Bucket, cfg.Prefix, cfg.Region)

	return &S3Client{
		client: s3.NewFromConfig(awsCfg, s3Opts...),
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// PutObject writes data to s3://<bucket>/<prefix><key>.
func (c *S3Client) PutObject(ctx context.Context, key string, data []byte) (int64, error) {
	fullKey := c.prefix + key
	logger.Get().Debug("s3: PutObject %s (%d bytes)", fullKey, len(data))

	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(c.bucket),
		Key:             aws.String(fullKey),
		Body:            bytes.NewReader(data),
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 put %s: %w", fullKey, err)
	}
	return int64(len(data)), nil
}

// GetObject fetches the object at s3://<bucket>/<prefix><key>.
func (c *S3Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	fullKey := c.prefix + key
	logger.Get().Debug("s3: GetObject %s", fullKey)

	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", fullKey, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// GetObjectRaw fetches an object by its full S3 key (no prefix prepended).
// Use this when you already have the full key from ListObjects.
func (c *S3Client) GetObjectRaw(ctx context.Context, fullKey string) ([]byte, error) {
	logger.Get().Debug("s3: GetObjectRaw %s", fullKey)

	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", fullKey, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// ListObjects returns all keys under s3://<bucket>/<prefix><keyPrefix>.
func (c *S3Client) ListObjects(ctx context.Context, keyPrefix string) ([]string, error) {
	fullPrefix := c.prefix + keyPrefix
	logger.Get().Debug("s3: ListObjects prefix=%s", fullPrefix)

	var keys []string

	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", fullPrefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}

	logger.Get().Debug("s3: ListObjects prefix=%s returned %d keys", fullPrefix, len(keys))
	return keys, nil
}
