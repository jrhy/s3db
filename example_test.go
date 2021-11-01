package s3db_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/jrhy/mast"
	"github.com/jrhy/s3db"
)

func s3Config(endpoint string) *aws.Config {
	return &aws.Config{
		Credentials: credentials.NewStaticCredentials(
			"TEST-ACCESSKEYID",
			"TEST-SECRETACCESSKEY",
			"",
		),
		Region:           aws.String("ca-west-1"),
		Endpoint:         &endpoint,
		S3ForcePathStyle: aws.Bool(true),
	}
}

func setupS3(bucketName string) (*aws.Config, func()) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	s3cfg, closer := s3Config(ts.URL), ts.Close

	c := s3.New(session.New(s3cfg))
	_, err := c.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		panic(err)
	}

	return s3cfg, closer
}

func Example() {
	ctx := context.Background()
	s3cfg, close := setupS3("bucket")
	defer close()

	c := s3.New(session.New(s3cfg))

	cfg := s3db.Config{
		Storage: &s3db.S3BucketInfo{
			EndpointURL: c.Endpoint,
			BucketName:  "bucket",
			Prefix:      "/my-awesome-database",
		},
		KeysLike:      "key",
		ValuesLike:    1234,
		NodeCache:     mast.NewNodeCache(1024),
		NodeEncryptor: s3db.V1NodeEncryptor([]byte("This is a secret passphrase if ever there was one.")),
	}
	s, err := s3db.Open(ctx, c, cfg, s3db.OpenOptions{}, time.Now())
	if err != nil {
		panic(err)
	}

	// setting a value
	err = s.Set(ctx, time.Now(), "hello", 5)
	if err != nil {
		panic(err)
	}

	// getting a value
	var v int
	ok, err := s.Get(ctx, "hello", &v)
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("how is that not OK?")
	}
	fmt.Printf("hello=%d\n", v)

	_, err = s.Commit(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Printf("size %d\n", s.Size())
	// Output:
	// hello=5
	// size 1
}
