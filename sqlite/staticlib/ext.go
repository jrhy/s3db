package main

import (
	_ "github.com/jrhy/s3db/sqlite/mod"
)

//go:generate sh -c "go build -buildmode=c-archive -o s3db.a"

// placeholder for c-archive
func main() {}
