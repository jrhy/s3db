
all:
	cd sharedlib && CGO_ENABLED=1 go generate

start-minio:
	if ! which minio ; then \
		go install -v github.com/minio/minio@latest ; \
		go install -v github.com/minio/mc@latest ; \
	fi
	if ! pgrep minio ; then \
		( MINIO_ROOT_USER=minioadmin \
		MINIO_ROOT_PASSWORD=miniopassword \
		minio server \
			--address 127.0.0.1:9091 \
			--console-address 127.0.0.1:9092 \
			$$(mktemp -d) & ) ; \
		sleep 3 ; \
		mc alias set mys3db http://127.0.0.1:9091 minioadmin miniopassword ; \
		mc ls mys3db/mybucket || mc mb mys3db/mybucket ; \
		fi

MINIO_ENV=\
	AWS_ACCESS_KEY_ID=minioadmin \
	AWS_SECRET_ACCESS_KEY=miniopassword \
	AWS_REGION=dummy \
	S3_ENDPOINT='http://127.0.0.1:9091'

run: all scratch.sqlite
	export ${MINIO_ENV} ; sqlite3 scratch.sqlite \
		-cmd '.load sharedlib/s3db'

scratch.sqlite:
	export ${MINIO_ENV} ; sqlite3 scratch.sqlite \
		-cmd '.load sharedlib/s3db' \
		"create virtual table if not exists mytable using s3db \
		   (node_cache_entries=1000, \
		    s3_bucket='mybucket', \
		    s3_endpoint='$$S3_ENDPOINT', \
		    s3_prefix='mytable', \
		    columns='id PRIMARY KEY, name, email')"
