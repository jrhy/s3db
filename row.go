package s3db

import (
	"time"

	v1proto "github.com/jrhy/s3db/proto/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func DeleteUpdateTime(baseTime time.Time, offset *durationpb.Duration) time.Time {
	return baseTime.Add(offset.AsDuration())
}
func UpdateTime(baseTime time.Time, cv *v1proto.ColumnValue) time.Time {
	return baseTime.Add(cv.UpdateOffset.AsDuration())
}
