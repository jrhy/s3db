package s3db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jrhy/mast"
	"github.com/jrhy/mast/persist/s3test"
	"github.com/jrhy/s3db/kv"
	v1proto "github.com/jrhy/s3db/proto/v1"
)

type KV struct {
	Root   *kv.DB
	Closer func()
}

var (
	inMemoryS3Lock sync.Mutex
	inMemoryS3 *s3.S3
	inMemoryBucket string
)

func OpenKV(ctx context.Context, s3opts S3Options, subdir string) (*KV, error) {
	var err error
	var c kv.S3Interface
	var closer func()
	if s3opts.Bucket == "" {
		if s3opts.Endpoint != "" {
			return nil, fmt.Errorf("s3_endpoint specified without s3_bucket")
		}
		inMemoryS3Lock.Lock()
		defer inMemoryS3Lock.Unlock()
		if inMemoryS3 == nil {
			inMemoryS3, inMemoryBucket, _ = s3test.Client()
		}
		s3opts.Endpoint = inMemoryS3.Endpoint
		s3opts.Bucket = inMemoryBucket
		c = inMemoryS3
	} else {
		c, err = getS3(s3opts.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("s3 client: %w", err)
		}
	}
	path := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(s3opts.Prefix, "/"), "/")+"/"+strings.TrimPrefix(subdir, "/"), "/")

	cfg := kv.Config{
		Storage: &kv.S3BucketInfo{
			EndpointURL: s3opts.Endpoint,
			BucketName:  s3opts.Bucket,
			Prefix:      path,
		},
		KeysLike:                     &Key{},
		ValuesLike:                   &v1proto.Row{},
		CustomMerge:                  mergeValues,
		CustomMarshal:                marshalProto,
		CustomUnmarshal:              unmarshalProto,
		MastNodeFormat:               string(mast.V1Marshaler),
		UnmarshalUsesRegisteredTypes: true,
	}
	if s3opts.NodeCacheEntries > 0 {
		cfg.NodeCache = mast.NewNodeCache(s3opts.NodeCacheEntries)
	}
	if s3opts.EntriesPerNode > 0 {
		cfg.BranchFactor = uint(s3opts.EntriesPerNode)
	}
	openOpts := kv.OpenOptions{
		ReadOnly:     s3opts.ReadOnly,
		OnlyVersions: s3opts.OnlyVersions,
	}
	s, err := kv.Open(ctx, c, cfg, openOpts, time.Now())
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	dbg("%s size:%d\n", subdir, s.Size())
	return &KV{
		Root:   s,
		Closer: closer,
	}, nil
}

type S3Options struct {
	Bucket   string
	Endpoint string
	Prefix   string

	EntriesPerNode   int
	NodeCacheEntries int
	ReadOnly         bool
	OnlyVersions     []string
}

func getS3(endpoint string) (*s3.S3, error) {
	config := aws.Config{}
	if endpoint != "" {
		config.Endpoint = &endpoint
		config.S3ForcePathStyle = aws.Bool(true)
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		return nil, fmt.Errorf("session: %w", err)
	}

	return s3.New(sess), nil
}
