package mod

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.riyazali.net/sqlite"

	"github.com/jrhy/s3db"
	"github.com/jrhy/s3db/kv"
	"github.com/jrhy/s3db/writetime"
)

type S3DBConn struct {
	ctx              context.Context
	ctxCancel        func()
	deadline         time.Time
	writeTime        time.Time
	txFixedWriteTime bool
}

func (sc *S3DBConn) ResetContext() {
	if sc.ctxCancel != nil {
		sc.ctxCancel()
		sc.ctxCancel = nil
	}
	sc.ctx = context.Background()
	if !sc.deadline.IsZero() {
		sc.ctx, sc.ctxCancel = context.WithDeadline(sc.ctx, sc.deadline)
	}
	if !sc.writeTime.IsZero() {
		sc.ctx = writetime.NewContext(sc.ctx, sc.writeTime)
	}
}

type Module struct {
	sc *S3DBConn
}

func (c *Module) Connect(conn *sqlite.Conn, args []string,
	declare func(string) error) (sqlite.VirtualTable, error) {

	args = args[2:]
	table, err := s3db.New(c.sc.ctx, args)
	if err != nil {
		return nil, err
	}

	err = declare(table.SchemaString)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	vt := &VirtualTable{
		module: c,
		common: table,
	}

	return vt, nil
}

func (c *Module) Create(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	// fmt.Printf("CREATE\n")
	return c.Connect(conn, args, declare)
}

type VirtualTable struct {
	common *s3db.VirtualTable
	module *Module
}

var _ sqlite.TwoPhaseCommitter = (*VirtualTable)(nil)

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
	return &Cursor{
		common: common,
		ctx:    c.module.sc.ctx,
	}, nil
}

func (c *VirtualTable) Disconnect() error {
	if err := toSqlite(c.common.Disconnect()); err != nil {
		return err
	}
	if c.module.sc.ctxCancel != nil {
		c.module.sc.ctxCancel()
		c.module.sc.ctxCancel = nil
	}

	return nil
}

func (c *VirtualTable) Destroy() error {
	return c.Disconnect()
}

type Cursor struct {
	common *s3db.Cursor
	ctx    context.Context
}

func (c *Cursor) Next() error {
	return toSqlite(c.common.Next(c.ctx))
}

func (c *Cursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	v, err := c.common.Column(i)
	if err != nil {
		return toSqlite(err)
	}
	setContextResult(ctx, v, i)
	return nil
}

func setContextResult(ctx *sqlite.VirtualTableContext, v interface{}, colIndex int) {
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
		ctx.ResultError(fmt.Errorf("column %d: cannot convert %T", colIndex, x))
	}
}

func (c *Cursor) Filter(_ int, idxStr string, values ...sqlite.Value) error {
	es := make([]interface{}, len(values))
	for i := range values {
		es[i] = valueToGo(values[i])
	}
	return toSqlite(c.common.Filter(c.ctx, idxStr, es))
}
func (c *Cursor) Rowid() (int64, error) {
	i, err := c.common.Rowid()
	return i, toSqlite(err)
}
func (c *Cursor) Eof() bool    { return c.common.Eof() }
func (c *Cursor) Close() error { return toSqlite(c.common.Close()) }

func init() {
	sqlite.Register(func(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
		sc := &S3DBConn{
			ctx: context.Background(),
		}
		err := api.CreateModule("s3db", &Module{sc},
			sqlite.ReadOnly(false), sqlite.Transaction(true),
			sqlite.TwoPhaseCommit(true))
		if err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		err = api.CreateModule("s3db_changes", &ChangesModule{sc})
		if err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		err = api.CreateModule("s3db_conn", &ConnModule{sc},
			sqlite.ReadOnly(false),
			sqlite.EponymousOnly(true))
		if err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		if err := api.CreateFunction("s3db_refresh", &RefreshFunc{sc}); err != nil {
			return sqlite.SQLITE_ERROR, fmt.Errorf("s3db_refresh: %w", err)
		}
		err = api.CreateModule("s3db_vacuum", &VacuumModule{sc}, sqlite.EponymousOnly(true))
		if err != nil {
			return sqlite.SQLITE_ERROR, fmt.Errorf("s3db_vacuum: %w", err)
		}
		if err := api.CreateFunction("s3db_version", &VersionFunc{sc}); err != nil {
			return sqlite.SQLITE_ERROR, fmt.Errorf("s3db_version: %w", err)
		}
		return sqlite.SQLITE_OK, nil
	})
}

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
	i, err := c.common.Insert(c.module.sc.ctx, valuesToGo(values))
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
	return toSqlite(c.common.Update(c.module.sc.ctx, valueToGo(value), valuesToGo(values)))
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
		backup, err = c.common.Tree.Root.Clone(c.module.sc.ctx)
		if err != nil {
			return fmt.Errorf("clone: %w", err)
		}
		if err := c.common.Delete(c.module.sc.ctx, valueToGo(oldValue)); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}
	if _, err := c.common.Insert(c.module.sc.ctx, valuesToGo(values)); err != nil {
		c.common.Tree.Root.Cancel()
		c.common.Tree.Root = backup
		return fmt.Errorf("update: %w", err)
	}
	backup.Cancel()
	return nil
}

func (c *VirtualTable) Delete(value sqlite.Value) error {
	return toSqlite(c.common.Delete(c.module.sc.ctx, valueToGo(value)))
}

func (c *VirtualTable) Begin() error {
	if c.module.sc.writeTime.IsZero() {
		c.module.sc.writeTime = time.Now()
		c.module.sc.txFixedWriteTime = true
		c.module.sc.ResetContext()
	}
	return toSqlite(c.common.Begin(c.module.sc.ctx))
}

func (c *VirtualTable) Commit() error {
	if c.module.sc.txFixedWriteTime {
		c.module.sc.writeTime = time.Time{}
		c.module.sc.txFixedWriteTime = false
		c.module.sc.ResetContext()
	}
	return nil
}

func (c *VirtualTable) Sync() error {
	if c.common.S3Options.ReadOnly {
		return nil
	}

	return toSqlite(c.common.Commit(c.module.sc.ctx))
}

func (c *VirtualTable) Rollback() error {
	res := toSqlite(c.common.Rollback())
	if c.module.sc.txFixedWriteTime {
		c.module.sc.writeTime = time.Time{}
		c.module.sc.txFixedWriteTime = false
		c.module.sc.ResetContext()
	}
	return res
}
