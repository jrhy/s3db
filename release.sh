#!/usr/bin/env bash 

SQLITE3_VERSION=3460100

set -o errexit
set -o pipefail
set -o nounset
set -x

mkdir release

go version

go generate ./proto
if git diff | egrep '^[+-]' | egrep -v '^---|\+\+\+' | egrep -v '^.//' ; then
	git diff --name-only --exit-code || (echo "The files above need updating. Please run 'go generate'."; exit 1)
fi

go get -t ./...
go vet ./...
go test -race ./...
go mod tidy
git diff --name-only --exit-code go.mod || (echo "Please run 'go mod tidy'."; exit 1)

# install zig and qemu-user
which zig || ( cd /tmp && curl -LO https://ziglang.org/download/0.9.1/zig-linux-aarch64-0.9.1.tar.xz && ( xzcat zig*xz | tar xf - ) && cd zig*/ && ln -s `pwd`/zig /usr/bin/zig )
which qemu-arm || ( sudo apt-get -y update && sudo apt-get -y install qemu-user libc6-armhf-cross libc6-arm64-cross libc6-amd64-cross )

# cross-compile extensions
cd sqlite/sharedlib
CGO_ENABLED=1 GOOS=linux GOARCH=arm CC="zig cc -target arm-linux-gnueabihf" go generate && mv s3db.so ../../release/s3db-linux-arm-glibc.sqlite-ext.so
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC="zig cc -target aarch64-linux-gnu" go generate && mv s3db.so ../../release/s3db-linux-arm64-glibc.sqlite-ext.so
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC="zig cc -target x86_64-linux-gnu" go generate && mv s3db.so ../../release/s3db-linux-amd64-glibc.sqlite-ext.so
#segfaults CGO_ENABLED=1 GOOS=linux GOARCH=386 GO386=sse2 CC="zig cc -target x86-linux-gnu" go generate && mv s3db.so ../../release/s3db-linux-386-glibc.sqlite-ext.so
#ld.lld: error: .cache/zig/o/e51b22516508da4ed5a02967b5ed4c8c/_cgo_export.o is incompatible with elf32_x86_64
#CGO_ENABLED=1 GOOS=linux GOARCH=386 CC="zig cc -target x86_64-linux-gnux32" go generate && mv s3db.so ../../release/s3db-linux-386-glibc.sqlite-ext.so
cd ../..

# cross-compile sqlite
if ! [ -f /tmp/sqlite-amalgamation-$SQLITE3_VERSION/sqlite-arm ] ; then
	pushd /tmp
	curl -LO https://www.sqlite.org/2024/sqlite-amalgamation-$SQLITE3_VERSION.zip
	unzip sqlite-amalgamation-$SQLITE3_VERSION.zip
	cd sqlite-amalgamation-$SQLITE3_VERSION
	zig cc -target arm-linux-gnueabihf -o sqlite-arm *.c
	zig cc -target aarch64-linux-gnu -o sqlite-arm64 *.c
	zig cc -target x86_64-linux-gnu -o sqlite-amd64 *.c
	popd
fi

# verify each target can load the extension
set +o pipefail
( qemu-arm -L /usr/arm-linux-gnueabihf/ /tmp/sqlite-amalgamation-$SQLITE3_VERSION/sqlite-arm -bail -cmd ".load release/s3db-linux-arm-glibc.sqlite-ext.so" -cmd "create virtual table f using s3db" 2>&1 | grep 'columns and constraints' ) || ( echo failed to load s3db extension for arm ; exit 1 )
( qemu-aarch64 -L /usr/aarch64-linux-gnu/ /tmp/sqlite-amalgamation-$SQLITE3_VERSION/sqlite-arm64 -bail -cmd ".load release/s3db-linux-arm64-glibc.sqlite-ext.so" -cmd "create virtual table f using s3db" 2>&1 | grep 'columns and constraints' ) || ( echo failed to load s3db extension for arm64 ; exit 1 )
( qemu-x86_64 -L /usr/x86_64-linux-gnu/ /tmp/sqlite-amalgamation-$SQLITE3_VERSION/sqlite-amd64 -bail -cmd ".load release/s3db-linux-amd64-glibc.sqlite-ext.so" -cmd "create virtual table f using s3db" 2>&1 | grep 'columns and constraints' ) || ( echo failed to load s3db extension for amd64 ; exit 1 )

cd release
gzip -9 *

