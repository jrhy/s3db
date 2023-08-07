package mod_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/jrhy/mast/persist/s3test"
	"github.com/jrhy/s3db/test"

	// register s3db extension with riyazali.net/sqlite
	_ "github.com/jrhy/s3db/sqlite"
	// autoload riyazali.net/sqlite-registered extensions in sqlite
	_ "github.com/jrhy/s3db/sqlite/sqlite-autoload-extension"
	// mattn's awesome sql driver for sqlite
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	os.Setenv("AWS_REGION", "dummy")
	os.Setenv("AWS_ACCESS_KEY_ID", "dummy")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "dummy")
}

type extension struct{}

func (e *extension) OpenDB() (*sql.DB, string, string) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}

	c, s3Bucket, _ := s3test.Client()

	return db, s3Bucket, c.Endpoint
}

func Test(t *testing.T) {
	if os.Getenv("AWS_REGION") == "" {
		t.Skip("skipping tests meant to be run from Makefile")
		return
	}
	test.Test(t, &extension{})
}
