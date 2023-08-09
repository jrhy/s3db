package mod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.riyazali.net/sqlite"

	"github.com/jrhy/mast"
	"github.com/jrhy/s3db"
	"github.com/jrhy/s3db/internal"
	"github.com/jrhy/s3db/kv"
	v1proto "github.com/jrhy/s3db/proto/v1"
)

type ChangesModule struct{}

func (c *ChangesModule) Connect(conn *sqlite.Conn, args []string,
	declare func(string) error) (sqlite.VirtualTable, error) {

	var err error
	res := &ChangesTable{}
	args = args[3:]

	if len(args) == 0 {
		// columns='<colname> [type] [primary key] [not null], ...',
		return nil, errors.New(`
usage:
 from='["version1"]                starting version for getting changes, from s3db_version()
 table='...',                      table to get changes for, must already be loaded
[to='["version2"]']                ending version for getting changes, from s3db_version()`)
	}

	seen := map[string]struct{}{}
	var fromVer, tableName, toVer string
	for i := range args {
		s := strings.SplitN(args[i], "=", 2)
		if _, ok := seen[s[0]]; ok {
			return nil, fmt.Errorf("duplicated: %s", s[0])
		}
		seen[s[0]] = struct{}{}
		switch s[0] {
		case "from":
			fromVer = internal.UnquoteAll(s[1])
		case "table":
			tableName = internal.UnquoteAll(s[1])
		case "to":
			toVer = internal.UnquoteAll(s[1])
		}
	}
	if tableName == "" {
		return nil, errors.New("no table specified")
	}
	tableName = internal.UnquoteAll(tableName)
	res.table = s3db.GetTable(tableName)
	if res.table == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	err = parseVersions(fromVer, &res.fromVer)
	if err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}
	err = parseVersions(toVer, &res.toVer)
	if err != nil {
		return nil, fmt.Errorf("to: %w", err)
	}
	// Tables aren't loaded on CREATE because we don't want to get in the
	// way of DROP TABLE if there is some kind of problem.

	err = declare(res.table.SchemaString)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	return res, nil
}

func parseVersions(s string, res *[]string) error {
	if s == "" {
		return nil
	}
	return json.Unmarshal([]byte(s), res)
}

func (c *ChangesModule) Create(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	// fmt.Printf("CREATE\n")
	return c.Connect(conn, args, declare)
}

func (c *ChangesTable) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	used := make([]*sqlite.ConstraintUsage, len(input.Constraints))
	return &sqlite.IndexInfoOutput{
		EstimatedCost:   1,
		ConstraintUsage: used,
	}, nil
}

type ChangesTable struct {
	table *s3db.VirtualTable

	fromVer []string
	toVer   []string
}

func loadForDiffing(ctx context.Context, baseOptions s3db.S3Options, versions []string) (*s3db.KV, error) {
	options := baseOptions
	options.ReadOnly = true
	options.OnlyVersions = versions
	return s3db.OpenKV(ctx, options, "s3db-rows")
}

func (c *ChangesTable) Open() (sqlite.VirtualCursor, error) {
	var from *s3db.KV
	var err error
	if c.fromVer == nil {
		from = c.table.Tree
	} else {
		from, err = loadForDiffing(c.table.Ctx, c.table.S3Options, c.fromVer)
		if err != nil {
			return nil, fmt.Errorf("from: %w", err)
		}
	}
	to, err := loadForDiffing(c.table.Ctx, c.table.S3Options, c.toVer)
	if err != nil {
		return nil, fmt.Errorf("to: %w", err)
	}

	dc, err := to.Root.StartDiff(c.table.Ctx, from.Root)
	if err != nil {
		return nil, toSqlite(err)
	}
	return &ChangesCursor{
		t:          c.table,
		diffCursor: dc,
	}, nil
}

func (c *ChangesTable) Disconnect() error {
	return nil
}

func (c *ChangesTable) Destroy() error {
	return nil
}

type ChangesCursor struct {
	t          *s3db.VirtualTable
	currentKey *s3db.Key
	currentRow *v1proto.Row
	diffCursor *kv.DiffCursor
	eof        bool
}

func (c *ChangesCursor) Next() error {
	if c.eof {
		return nil
	}
	for {
		de, err := c.diffCursor.NextEntry(c.t.Ctx)
		if err == mast.ErrNoMoreDiffs {
			c.eof = true
			return nil
		}
		if de.NewValue != nil {
			c.currentRow = de.NewValue.(*v1proto.Row)
			c.currentKey = de.Key.(*s3db.Key)
			return nil
		}
	}
}

func (c *ChangesCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	if c.currentRow.Deleted {
		return fmt.Errorf("accessing deleted row")
	}
	var res interface{}
	if i == c.t.KeyCol {
		res = c.currentKey.Value()
	} else if cv, ok := c.currentRow.ColumnValues[c.t.ColumnNameByIndex[i]]; ok {
		res = s3db.FromSQLiteValue(cv.Value)
	}
	setContextResult(ctx, res, i)
	return nil
}

func (c *ChangesCursor) Filter(_ int, idxStr string, values ...sqlite.Value) error {
	return toSqlite(c.Next())
}
func (c *ChangesCursor) Rowid() (int64, error) {
	return 0, errors.New("rowid: not expecting to be called, source table should be WITHOUT ROWID")
}
func (c *ChangesCursor) Eof() bool    { return c.eof }
func (c *ChangesCursor) Close() error { return nil }
