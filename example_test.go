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

func ExampleOpen() {
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
		KeysLike:   "key",
		ValuesLike: 1234,
	}
	s, err := s3db.Open(ctx, c, cfg, s3db.OpenOptions{}, time.Now())
	if err != nil {
		panic(err)
	}
	err = s.Set(ctx, time.Now(), "hello", 5)
	if err != nil {
		panic(err)
	}
	_, err = s.Commit(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d\n", s.Size())
	// Output:
	// 1
}
