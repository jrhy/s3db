package main

import (
	"errors"
	"fmt"

	"go.riyazali.net/sqlite"

	"github.com/jrhy/s3db"
	"github.com/jrhy/s3db/kv"
)

//go:generate sh -c "go build -buildmode=c-shared -o `if [ \"$GOOS\" = \"darwin\" ] ; then echo s3db.dylib ; else echo s3db.so ; fi`"

type Module struct{}

func (c *Module) Connect(conn *sqlite.Conn, args []string,
	declare func(string) error) (sqlite.VirtualTable, error) {

	args = args[2:]
	table, err := s3db.New(args)
	if err != nil {
		return nil, err
	}

	err = declare(table.SchemaString)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	vt := &VirtualTable{common: table}

	return vt, nil
}

func (c *Module) Create(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	// fmt.Printf("CREATE\n")
	return c.Connect(conn, args, declare)
}

type VirtualTable struct {
	common *s3db.VirtualTable
}

func mapOp(in sqlite.ConstraintOp, usable bool) s3db.Op {
	if !usable {
		return s3db.OpIgnore
	}
	switch in {
	case sqlite.INDEX_CONSTRAINT_EQ:
		return s3db.OpEQ
	case sqlite.INDEX_CONSTRAINT_GT:
		return s3db.OpGT
	case sqlite.INDEX_CONSTRAINT_GE:
		return s3db.OpGE
	case sqlite.INDEX_CONSTRAINT_LT:
		return s3db.OpLT
	case sqlite.INDEX_CONSTRAINT_LE:
		return s3db.OpLE
	}
	return s3db.OpIgnore
}

func (c *VirtualTable) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	indexIn := make([]s3db.IndexInput, len(input.Constraints))
	for i, c := range input.Constraints {
		op := mapOp(c.Op, c.Usable)
		indexIn[i] = s3db.IndexInput{
			ColumnIndex: c.ColumnIndex,
			Op:          op,
		}
	}
	orderIn := make([]s3db.OrderInput, len(input.OrderBy))
	for i, o := range input.OrderBy {
		orderIn[i] = s3db.OrderInput{
			Column: o.ColumnIndex,
			Desc:   o.Desc,
		}
	}
	indexOut, err := c.common.BestIndex(indexIn, orderIn)
	if err != nil {
		return nil, toSqlite(err)
	}
	used := make([]*sqlite.ConstraintUsage, len(indexIn))
	for i := range indexOut.Used {
		if indexOut.Used[i] {
			used[i] = &sqlite.ConstraintUsage{
				ArgvIndex: i + 1,
				//Omit: true, // no known cases where this doesn't work, but...
			}
		}
	}
	return &sqlite.IndexInfoOutput{
		EstimatedCost:   indexOut.EstimatedCost,
		ConstraintUsage: used,
		OrderByConsumed: indexOut.AlreadyOrdered,
		IndexNumber:     indexOut.IdxNum,
		IndexString:     indexOut.IdxStr,
	}, nil
}

func (c *VirtualTable) Open() (sqlite.VirtualCursor, error) {
	common, err := c.common.Open()
	if err != nil {
		return nil, toSqlite(err)
	}
	return &Cursor{common: common}, nil
}

func (c *VirtualTable) Disconnect() error {
	if err := toSqlite(c.common.Disconnect()); err != nil {
		return err
	}

	return nil
}

func (c *VirtualTable) Destroy() error {
	return c.Disconnect()
}

type Cursor struct {
	common *s3db.Cursor
}

func (c *Cursor) Next() error {
	return toSqlite(c.common.Next())
}

func (c *Cursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	v, err := c.common.Column(i)
	if err != nil {
		return toSqlite(err)
	}
	switch x := v.(type) {
	case nil:
		ctx.ResultNull()
	case []byte:
		ctx.ResultBlob(x)
	case float64:
		ctx.ResultFloat(x)
	case int:
		ctx.ResultInt(x)
	case int64:
		ctx.ResultInt64(x)
	case string:
		ctx.ResultText(x)
	default:
		ctx.ResultError(fmt.Errorf("column %d: cannot convert %T", i, x))
	}
	return nil
}

func (c *Cursor) Filter(_ int, idxStr string, values ...sqlite.Value) error {
	es := make([]interface{}, len(values))
	for i := range values {
		es[i] = valueToGo(values[i])
	}
	return toSqlite(c.common.Filter(idxStr, es))
}
func (c *Cursor) Rowid() (int64, error) {
	i, err := c.common.Rowid()
	return i, toSqlite(err)
}
func (c *Cursor) Eof() bool    { return c.common.Eof() }
func (c *Cursor) Close() error { return toSqlite(c.common.Close()) }

func init() {
	sqlite.Register(func(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
		err := api.CreateModule("s3db", &Module{},
			func(opts *sqlite.ModuleOptions) {
				opts.ReadOnly = false
				opts.Transactional = true
			})
		if err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		err = api.CreateModule("s3db_vacuum", &VacuumModule{},
			func(opts *sqlite.ModuleOptions) {
				opts.ReadOnly = true
				opts.EponymousOnly = true
			})
		if err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		return sqlite.SQLITE_OK, nil
	})
}

// placeholder for c-shared
func main() {}

func valuesToGo(values []sqlite.Value) map[int]interface{} {
	res := make(map[int]interface{}, len(values))
	for i := range values {
		if values[i].NoChange() {
			continue
		}
		res[i] = valueToGo(values[i])
	}
	return res
}
func valueToGo(value sqlite.Value) interface{} {
	switch value.Type() {
	case sqlite.SQLITE_BLOB:
		return value.Blob()
	case sqlite.SQLITE_FLOAT:
		return value.Float()
	case sqlite.SQLITE_INTEGER:
		return value.Int64()
	case sqlite.SQLITE_NULL:
		return nil
	case sqlite.SQLITE_TEXT:
		return value.Text()
	default:
		panic(fmt.Sprintf("cannot convert type %d", value.Type()))
	}
}

func (c *VirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	i, err := c.common.Insert(valuesToGo(values))
	return i, toSqlite(err)
}

func toSqlite(err error) error {
	switch err {
	case s3db.ErrS3DBConstraintNotNull:
		return sqlite.SQLITE_CONSTRAINT_NOTNULL
	case s3db.ErrS3DBConstraintPrimaryKey:
		return sqlite.SQLITE_CONSTRAINT_PRIMARYKEY
	case s3db.ErrS3DBConstraintUnique:
		return sqlite.SQLITE_CONSTRAINT_UNIQUE
	default:
		return err
	}
}

func (c *VirtualTable) Update(value sqlite.Value, values ...sqlite.Value) error {
	return toSqlite(c.common.Update(valueToGo(value), valuesToGo(values)))
}

func (c *VirtualTable) Replace(oldValue, newValue sqlite.Value, values ...sqlite.Value) error {
	if true {
		return errors.New("unimplemented")
	}
	fmt.Printf("REPLACE ")
	fmt.Printf("oldValue nochange=%v %s %+v  ", oldValue.NoChange(), oldValue.Type(), oldValue.Text())
	fmt.Printf("newValue nochange=%v %s %+v\n", newValue.NoChange(), newValue.Type(), newValue.Text())
	var backup *kv.DB
	var err error
	if newValue.NoChange() {
		newValue = oldValue
	} else {
		backup, err = c.common.Tree.Root.Clone(c.common.Ctx)
		if err != nil {
			return fmt.Errorf("clone: %w", err)
		}
		if err := c.common.Delete(valueToGo(oldValue)); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}
	if _, err := c.common.Insert(valuesToGo(values)); err != nil {
		c.common.Tree.Root.Cancel()
		c.common.Tree.Root = backup
		return fmt.Errorf("update: %w", err)
	}
	backup.Cancel()
	return nil
}

func (c *VirtualTable) Delete(value sqlite.Value) error {
	return toSqlite(c.common.Delete(valueToGo(value)))
}

func (c *VirtualTable) Begin() error {
	return toSqlite(c.common.Begin())
}

func (c *VirtualTable) Commit() error {
	return toSqlite(c.common.Commit())
}

func (c *VirtualTable) Rollback() error {
	return toSqlite(c.common.Rollback())
}
