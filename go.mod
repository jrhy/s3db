module github.com/jrhy/s3db

go 1.14

require (
	github.com/aws/aws-sdk-go v1.40.45
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/johannesboyne/gofakes3 v0.0.0-20210819161434-5c8dfcfe5310
	github.com/jrhy/mast v1.2.1
	github.com/kr/text v0.2.0 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/stretchr/testify v1.7.0
	golang.org/x/crypto v0.0.0-20210915214749-c084706c2272
	golang.org/x/sys v0.0.0-20210917161153-d61c044b1678 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)

replace github.com/jrhy/mast => ../mast
