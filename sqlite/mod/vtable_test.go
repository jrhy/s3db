// Cannot test on MacOS due to lack of -linkshared.

// +build !darwin

package mod_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/jrhy/mast/persist/s3test"
	"github.com/jrhy/s3db/test"
	"github.com/mattn/go-sqlite3"
)

func init() {
	driver := &sqlite3.SQLiteDriver{}
	driver.Extensions = []string{"./s3db"}

	sql.Register("sqlite3_with_extensions", driver)
}

type extension struct{}

func (e *extension) OpenDB() (*sql.DB, string, string) {
	db, err := sql.Open("sqlite3_with_extensions", ":memory:")
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
