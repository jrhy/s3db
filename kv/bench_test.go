package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jrhy/s3db/kv"
)

func BenchmarkSet1(b *testing.B) {
	ctx := context.Background()
	t, close := newTestTree(0, 0)
	defer close()
	for n := 0; n < b.N; n++ {
		t.Set(ctx, time.Time{}, 0, 0)
	}
	t.Cancel()
}

func BenchmarkSetN(b *testing.B) {
	ctx := context.Background()
	t, close := newTestTree(0, 0)
	defer close()
	for n := 0; n < b.N; n++ {
		t.Set(ctx, time.Time{}, n, n)
	}
	t.Cancel()
}

func BenchmarkSetNCommit(b *testing.B) {
	ctx := context.Background()
	t, close := newTestTree(0, 0)
	defer close()
	for n := 0; n < b.N; n++ {
		t.Set(ctx, time.Time{}, n, n)
	}
	t.Commit(ctx)
}

func BenchmarkGet1(b *testing.B) {
	ctx := context.Background()
	t, close := newTestTree(0, 0)
	defer close()
	t.Set(ctx, time.Time{}, 0, 0)
	var v int
	for n := 0; n < b.N; n++ {
		t.Get(ctx, n, &v)
	}
	t.Cancel()
}

func BenchmarkGetNMemory(b *testing.B) {
	ctx := context.Background()
	t, close := newTestTree(0, 0)
	defer close()
	for n := 0; n < b.N; n++ {
		t.Set(ctx, time.Time{}, n, n)
	}
	t.Cancel()
	var v int
	for n := 0; n < b.N; n++ {
		t.Get(ctx, n, &v)
	}
}

func BenchmarkGetNStored(b *testing.B) {
	ctx := context.Background()
	t, close := newTestTree(0, 0)
	defer close()
	for n := 0; n < b.N; n++ {
		t.Set(ctx, time.Time{}, n, n)
	}
	t.Commit(ctx)
	var v int
	for n := 0; n < b.N; n++ {
		t.Get(ctx, n, &v)
	}
}

func newTestTree(zeroKey, zeroValue interface{}) (*kv.DB, func()) {
	ctx := context.Background()
	s3cfg, close := setupS3("bucket")

	c := s3.New(session.New(s3cfg))

	cfg := kv.Config{
		Storage: &kv.S3BucketInfo{
			EndpointURL: c.Endpoint,
			BucketName:  "bucket",
			Prefix:      "/my-awesome-database",
		},
		KeysLike:   "key",
		ValuesLike: 1234,
	}
	s, err := kv.Open(ctx, c, cfg, kv.OpenOptions{}, time.Now())
	if err != nil {
		panic(err)
	}
	return s, close
}
