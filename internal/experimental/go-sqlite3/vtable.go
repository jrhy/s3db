//go:build sqlite_vtable

package main

import (
	"fmt"

	"github.com/jrhy/s3db"
	sqlite3 "github.com/mattn/go-sqlite3"
)

func ConnectHook(conn *sqlite3.SQLiteConn) error {
	err := conn.CreateModule("s3db", &Module{})
	if err != nil {
		return err
	}
	return conn.CreateModule("s3db_vacuum", &VacuumModule{})
}

type Module struct{}

func (m *Module) Create(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	args = args[2:]
	table, err := s3db.New(args)
	if err != nil {
		return nil, err
	}
	err = c.DeclareVTab(table.SchemaString)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}
	return &VirtualTable{common: table}, nil
}

type VirtualTable struct {
	common *s3db.VirtualTable
}

func (m *Module) Connect(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	return m.Create(c, args)
}

func (m *Module) DestroyModule() {}

func toSqlite(err error) error {
	switch err {
	case s3db.ErrS3DBConstraintNotNull:
		return sqlite3.ErrConstraintNotNull
	case s3db.ErrS3DBConstraintPrimaryKey:
		return sqlite3.ErrConstraintPrimaryKey
	case s3db.ErrS3DBConstraintUnique:
		return sqlite3.ErrConstraintUnique
	default:
		return err
	}
}

func (c *VirtualTable) Open() (sqlite3.VTabCursor, error) {
	common, err := c.common.Open()
	if err != nil {
		return nil, toSqlite(err)
	}
	return &Cursor{common: common}, nil
}

func mapOp(in sqlite3.Op, usable bool) s3db.Op {
	if !usable {
		return s3db.OpIgnore
	}
	switch in {
	case sqlite3.OpEQ:
		return s3db.OpEQ
	case sqlite3.OpGT:
		return s3db.OpGT
	case sqlite3.OpGE:
		return s3db.OpGE
	case sqlite3.OpLT:
		return s3db.OpLT
	case sqlite3.OpLE:
		return s3db.OpLE
	}
	return s3db.OpIgnore
}

func (c *VirtualTable) BestIndex(cst []sqlite3.InfoConstraint, ob []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	indexIn := make([]s3db.IndexInput, len(cst))
	for i, c := range cst {
		indexIn[i] = s3db.IndexInput{
			ColumnIndex: c.Column,
			Op:          mapOp(c.Op, c.Usable),
		}
	}
	orderIn := make([]s3db.OrderInput, len(ob))
	for i, o := range ob {
		orderIn[i] = s3db.OrderInput{
			Column: o.Column,
			Desc:   o.Desc,
		}
	}
	indexOut, err := c.common.BestIndex(indexIn, orderIn)
	if err != nil {
		return nil, toSqlite(err)
	}
	return &sqlite3.IndexResult{
		IdxNum:         indexOut.IdxNum,
		IdxStr:         indexOut.IdxStr,
		AlreadyOrdered: indexOut.AlreadyOrdered,
		Used:           indexOut.Used,
		EstimatedCost:  indexOut.EstimatedCost,
	}, nil
}

func (c *VirtualTable) Disconnect() error { return toSqlite(c.common.Disconnect()) }
func (c *VirtualTable) Destroy() error    { return c.Disconnect() }

type Cursor struct {
	common *s3db.Cursor
}

func (c *Cursor) Column(ctx *sqlite3.SQLiteContext, i int) error {
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
		ctx.ResultDouble(x)
	case int:
		ctx.ResultInt(x)
	case int64:
		ctx.ResultInt64(x)
	case string:
		ctx.ResultText(x)
	default:
		return fmt.Errorf("column %d: cannot convert %T", i, v)
	}
	return nil
}

func (c *Cursor) Filter(_ int, idxStr string, vals []interface{}) error {
	return toSqlite(c.common.Filter(idxStr, vals))
}

func (c *Cursor) Rowid() (int64, error) {
	i, err := c.common.Rowid()
	return i, toSqlite(err)
}

func (c *Cursor) Next() error  { return toSqlite(c.common.Next()) }
func (c *Cursor) EOF() bool    { return c.common.Eof() }
func (c *Cursor) Close() error { return toSqlite(c.common.Close()) }

func (c *VirtualTable) Insert(key interface{}, values []interface{}) (int64, error) {
	i, err := c.common.Insert(valuesToGo(values))
	if err != nil {
		return i, toSqlite(err)
	}
	// go-sqlite3 doesn't yet support transactions, so commit immediately
	return 0, toSqlite(c.common.Commit())
}

func (c *VirtualTable) Delete(key interface{}) error {
	err := toSqlite(c.common.Delete(key))
	if err != nil {
		return err
	}
	// go-sqlite3 doesn't yet support transactions, so commit immediately
	return toSqlite(c.common.Commit())
}

func (c *VirtualTable) Update(key interface{}, values []interface{}) error {
	err := toSqlite(c.common.Update(key, valuesToGo(values)))
	if err != nil {
		return err
	}
	// go-sqlite3 doesn't yet support transactions, so commit immediately
	return toSqlite(c.common.Commit())
}

func valuesToGo(values []interface{}) map[int]interface{} {
	// TODO does the map[int] actually make sense if sqlite is going to replace all the column values anyway? check with a test.
	res := make(map[int]interface{}, len(values))
	for i := range values {
		res[i] = values[i]
	}
	return res
}

var _ sqlite3.VTabUpdater = (*VirtualTable)(nil)
