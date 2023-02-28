package s3db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jrhy/mast"
	v1proto "github.com/jrhy/s3db/proto/v1"
)

type Key struct {
	*v1proto.SQLiteValue
}

var _ mast.Key = &Key{}

func NewKey(i interface{}) *Key {
	switch x := i.(type) {
	case int:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case int32:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case int16:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case int8:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case int64:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case uint:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case uint8:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case uint16:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case uint32:
		var v int64 = int64(x)
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_INT, Int: v}}
	case float64:
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_REAL, Real: x}}
	case string:
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_TEXT, Text: x}}
	case []byte:
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_BLOB, Blob: x}}
	case nil:
		return &Key{&v1proto.SQLiteValue{Type: v1proto.Type_NULL}}
	default:
		panic(fmt.Errorf("unhandled Key type %T", x))
	}
}

var defaultLayer = mast.DefaultLayer(nil)

func (k *Key) Layer(branchFactor uint) uint8 {
	var layer uint8
	var err error
	v := k.SQLiteValue
	switch v.Type {
	case v1proto.Type_INT:
		layer, err = defaultLayer(v.Int, branchFactor)
	case v1proto.Type_REAL:
		layer, err = defaultLayer(strconv.FormatFloat(v.Real, 'b', -1, 64), branchFactor)
	case v1proto.Type_TEXT:
		layer, err = defaultLayer(v.Text, branchFactor)
	case v1proto.Type_BLOB:
		layer, err = defaultLayer(v.Blob, branchFactor)
	case v1proto.Type_NULL:
		layer = 0
	default:
		panic("unhandled Key type")
	}
	if err != nil {
		panic(err)
	}
	return layer
}

func (k *Key) IsNull() bool {
	return k.Type == v1proto.Type_NULL
}

func (k *Key) Order(o2 mast.Key) int {
	if o2 == nil {
		return 1
	}
	k2 := o2.(*Key)
	v := k.SQLiteValue
	v2 := k2.SQLiteValue
	var flip bool
	v, v2, flip = orderType(v, v2)
	if v.Type == v1proto.Type_INT {
		if v2.Type == v1proto.Type_INT {
			if v.Int < v2.Int {
				return order(flip, -1)
			} else if v.Int > v2.Int {
				return order(flip, 1)
			}
			return 0
		}
		if v2.Type == v1proto.Type_REAL {
			if float64(v.Int) < v2.Real {
				return order(flip, -1)
			} else if float64(v.Int) > v2.Real {
				return order(flip, 1)
			}
			return 0
		}
		return order(flip, -1)
	}
	if v.Type == v1proto.Type_REAL {
		if v2.Type == v1proto.Type_REAL {
			if v.Real < v2.Real {
				return order(flip, -1)
			} else if v.Real > v2.Real {
				return order(flip, 1)
			}
			return 0
		}
		return order(flip, -1)
	}
	if v.Type == v1proto.Type_TEXT {
		if v2.Type == v1proto.Type_TEXT {
			if v.Text < v2.Text {
				return order(flip, -1)
			} else if v.Text > v2.Text {
				return order(flip, 1)
			}
			return 0
		}
		return order(flip, -1)
	}
	if v.Type == v1proto.Type_BLOB {
		if v2.Type == v1proto.Type_BLOB {
			return order(flip, bytes.Compare(v.Blob, v2.Blob))
		}
	}
	panic(fmt.Errorf("key comparison %T, %T in unexpected order",
		k.Value(), k2.Value()))
}

func orderType(v, v2 *v1proto.SQLiteValue) (*v1proto.SQLiteValue, *v1proto.SQLiteValue, bool) {
	if typeIndex(v) <= typeIndex(v2) {
		return v, v2, false
	}
	return v2, v, true
}

func typeIndex(v *v1proto.SQLiteValue) int {
	if v.Type == v1proto.Type_INT {
		return 0
	}
	if v.Type == v1proto.Type_REAL {
		return 1
	}
	if v.Type == v1proto.Type_TEXT {
		return 2
	}
	if v.Type == v1proto.Type_BLOB {
		return 3
	}
	panic("unhandled key type")
}

func order(flip bool, cmp int) int {
	if cmp == 0 || !flip {
		return cmp
	}
	return -1 * cmp
}

func (k *Key) Value() interface{} {
	switch k.Type {
	case v1proto.Type_INT:
		return k.Int
	case v1proto.Type_TEXT:
		return k.Text
	case v1proto.Type_REAL:
		return k.Real
	case v1proto.Type_BLOB:
		return k.Blob
	}
	return nil
}

func (k *Key) String() string {
	return mustJSON(k)
}

func mustJSON(i interface{}) string {
	var b []byte
	var err error
	b, err = json.Marshal(i)
	if err != nil {
		panic(err)
	}
	if len(b) > 60 {
		b, err = json.MarshalIndent(i, " ", " ")
		if err != nil {
			panic(err)
		}
	}
	return string(b)
}
