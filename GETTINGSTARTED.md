Grab your bucket. We'll go kick the tires with the s3db CLI.

# Check Go version
[Install Go](https://golang.org/doc/install) 1.14+.

# Get a bucket
Skip if you already have the AWS SDK configured, or want to use some other 
S3-compatible storage. If you just want to kick the
tires, you can install `minio` locally by:

```
go get github.com/minio/minio
go install github.com/minio/minio
```
or follow the excellent minio [Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide.html).

# Get the `mc` command
Minio's `mc` is also amazing. It can be installed by:

```
go get github.com/minio/minio
go install github.com/minio/minio
```

or follow their [Minio Client Guide](https://docs.min.io/docs/minio-client-complete-guide.html).

# Install the `s3db` CLI
```
go get github.com/jrhy/s3db/cmd/s3db
go install github.com/jrhy/s3db/cmd/s3db
```

# Configure access to your bucket
## If not using minio
Configure the AWS SDK like [this](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html). Create your bucket. Then tell `mc` how to access it, like:
```
export AWS_REGION=<your region, like us-east-1>
( echo $AWS_ACCESS_KEY ; echo $AWS_SECRET_ACCESS_KEY ) | mc alias set hello https://s3.$AWS_REGION.amazonaws.com
```

## If using minio
```
minio server -address localhost:9000 `mktemp -d` &
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=none
export S3_ENDPOINT=http://localhost:9000
mc alias set hello http://localhost:9000 minioadmin minioadmin
```

# Create a bucket
```
export B=<your-unique-bucket-name>
mc mb hello/$B
```

# Use `s3db` to set and get entries
```
s3db --bucket $B --prefix db1 set foo=bar
s3db -b $B -p db1 set foo2=bar2
s3db -b $B -p db1 show
```

# Use `s3db` to track changes
```
v1=`s3db -b $B -p db1 merge`
s3db -b $B -p db1 set foo=bar999
s3db -b $B -p db1 diff $v1
```

# Use encryption
```
dd if=/dev/random of=master.key bs=32 count=1
s3db -b $B -p db2 -k master.key set foo=bar
s3db -b $B -p db2 -k master.key show
```

# Track changes of a specific value
```
s3db -b $B -p db2 -k master.key set foo=bar2
s3db -b $B -p db2 -k master.key set foo=bar3
s3db -b $B -p db2 -k master.key set foo=bar4
s3db -b $B -p db2 -k master.key trace-history foo
```

# Mark data as no longer relevant
```
s3db -b $B -p db2 -k master.key tombstone foo
s3db -b $B -p db2 -k master.key show
```

# Delete space from versions you don't care about
```
mc ls -r  hello/$B/db2
s3db -b $B -p db2 -k master.key delete-history --older-than=1s
mc ls -r  hello/$B/db2
```

