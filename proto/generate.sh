#!/bin/sh

set -euo pipefail

export GOPATH=/tmp/s3db/proto-gotools
export PATH="$(go env GOPATH)/bin:$PATH"

go install github.com/bufbuild/buf/cmd/buf@v1.50.0
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
buf generate

