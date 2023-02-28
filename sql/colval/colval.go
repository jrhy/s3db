package colval

import (
	"fmt"
	"strconv"
	"strings"
)

type ColumnValue interface {
	String() string
	ToBool() *bool
}

type Text string
type Real float64
type Int int64
type Blob []byte
type Null struct{}

func (v Text) String() string { return string(v) }
func (v Real) String() string {
	res := strconv.FormatFloat(float64(v), 'g', -1, 64)
	if !strings.Contains(res, ".") {
		// add trailing tenth to distinguish real value, if no real part present
		res += ".0"
	}
	return res
}
func (v Int) String() string  { return strconv.FormatInt(int64(v), 10) }
func (v Blob) String() string { return strconv.Quote(string(v)) }
func (v Null) String() string { return "NULL" }

func ToGo(cv ColumnValue) interface{} {
	switch x := cv.(type) {
	case Blob:
		return []byte(x)
	case Int:
		return int64(x)
	case Null:
		return nil
	case Real:
		return float64(x)
	case Text:
		return string(x)
	default:
		panic(fmt.Errorf("unhandled colval %T", cv))
	}
}
func (v Int) ToBool() *bool  { b := v != 0; return &b }
func (v Real) ToBool() *bool { b := v != 0.0; return &b }
func (v Null) ToBool() *bool { return nil }
func (v Text) ToBool() *bool { b := false; return &b }
func (v Blob) ToBool() *bool { b := false; return &b }

func ToBool(cv ColumnValue) *bool {
	switch v := (cv).(type) {
	case Int:
		b := v != 0
		return &b
	case Real:
		b := v != 0.0
		return &b
	case Null:
		return nil
	}
	b := false
	return &b
}
