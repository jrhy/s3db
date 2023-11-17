package writetime

import (
	"context"
	"time"
)

type keyType interface{}

var (
	i   int
	key keyType = &i
)

func FromContext(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(key).(time.Time)
	return t, ok
}

func NewContext(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, key, t)
}
