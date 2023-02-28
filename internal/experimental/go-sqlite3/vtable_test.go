//go:build sqlite_vtable

package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/jrhy/mast/persist/s3test"
	"github.com/jrhy/s3db/test"
	"github.com/mattn/go-sqlite3"
)

func init() {
	driver := &sqlite3.SQLiteDriver{}
	driver.ConnectHook = ConnectHook
	sql.Register("sqlite3_with_extensions", driver)
	fmt.Printf("registered\n")

	os.Setenv("AWS_SECRET_ACCESS_KEY", "dummy")
	os.Setenv("AWS_REGION", "dummy ")
	os.Setenv("AWS_ACCESS_KEY_ID", "dummy")
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
	test.Test(t, &extension{})
}
