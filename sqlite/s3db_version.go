package mod

import (
	"encoding/json"
	"fmt"

	"go.riyazali.net/sqlite"

	"github.com/jrhy/s3db"
)

type VersionFunc struct{}

func (h *VersionFunc) Args() int           { return 1 }
func (h *VersionFunc) Deterministic() bool { return false }
func (h *VersionFunc) Step(ctx *sqlite.AggregateContext, values ...sqlite.Value) {
	if ctx.Data() == nil {
		ctx.SetData(&VersionFuncContext{})
	}

	var val = values[0]
	var fCtx = ctx.Data().(*VersionFuncContext)

	if !val.IsNil() {
		fCtx.tableName = val.Text()
	}
}
func (h *VersionFunc) Final(ctx *sqlite.AggregateContext) {
	if ctx.Data() == nil {
		return
	}
	var fCtx = ctx.Data().(*VersionFuncContext)
	if fCtx.tableName == "" {
		ctx.ResultError(fmt.Errorf("missing table name"))
		return
	}
	vt := s3db.GetTable(fCtx.tableName)
	if vt == nil {
		ctx.ResultError(fmt.Errorf("table not found: %s", fCtx.tableName))
		return
	}
	roots, err := vt.Tree.Root.Roots()
	if err != nil {
		ctx.ResultError(err)
		return
	}
	if len(roots) == 0 {
		ctx.ResultText("[]")
	} else {
		var je []byte
		je, err = json.Marshal(roots)
		if err != nil {
			ctx.ResultError(err)
			return
		}
		ctx.ResultText(string(je))
	}
}

type VersionFuncContext struct {
	tableName string
}
