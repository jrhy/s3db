package main

import (
	_ "github.com/jrhy/s3db/sqlite"
)

//go:generate sh -c "go build -buildmode=c-shared -o `if [ \"$GOOS\" = \"darwin\" ] ; then echo s3db.dylib ; else echo s3db.so ; fi`"

// placeholder for c-shared
func main() {}
