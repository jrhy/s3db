#!/usr/bin/env bash 

set -o errexit
set -o pipefail
set -o nounset
set -x

go version
go get -t ./...
go vet -tags sqlite_vtable ./...
go test -race -tags sqlite_vtable ./...

go generate ./proto
git diff --name-only --exit-code || (echo "The files above need updating. Please run 'go generate'."; exit 1)

go mod tidy
git diff --name-only --exit-code || (echo "Please run 'go mod tidy'."; exit 1)

mkdir release
cd sqlite
go generate && mv s3db.so ../release/s3db-linux-amd64-glibc.sqlite-ext.so
CGO_ENABLED=1 GOOS=linux GOARCH=arm CC="zig cc -target arm-linux-gnueabihf" go generate && mv s3db.so ../release/s3db-linux-arm-glibc.sqlite-ext.so
cd ../release
gzip -9 *

