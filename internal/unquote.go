package internal

import (
	"github.com/jrhy/s3db/sql"
	"github.com/jrhy/s3db/sql/colval"
	"github.com/jrhy/s3db/sql/parse"
)

func UnquoteAll(s string) string {
	if len(s) == 0 {
		return ""
	}
	p := &parse.Parser{
		Remaining: s,
	}
	var cv colval.ColumnValue
	var res string
	for {
		if ok := sql.ColumnValueParser(&cv)(p); !ok {
			// dbg("skipping unquote; using: %s\n", s)
			return s
		}
		res += cv.String()
		if len(p.Remaining) == 0 {
			break
		}
	}
	// dbg("unquoted to: %s\n", res)
	return res
}
