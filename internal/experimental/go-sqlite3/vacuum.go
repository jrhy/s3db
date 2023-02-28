package sqlite3

import (
	"errors"
	"fmt"
	"time"

	"github.com/jrhy/s3db"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type VacuumModule struct{}

func (vm *VacuumModule) EponymousOnlyModule() {}

func (vm *VacuumModule) Create(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	return nil, errors.New("not implemented")
}

func (vm *VacuumModule) Connect(conn *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	err := conn.DeclareVTab(`create table s3db_vacuum(
		vacuum_error,
		table_name HIDDEN,
		before_time HIDDEN
	)`)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	return &VacuumModule{}, nil
}

func (vm *VacuumModule) DestroyModule() {}

func (vm *VacuumModule) BestIndex(cst []sqlite3.InfoConstraint, ob []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	used := make([]bool, len(cst))
	colUsed := make(map[int]bool)
	for i, c := range cst {
		if c.Column >= 1 && c.Column <= 2 && c.Op == sqlite3.OpEQ {
			colUsed[c.Column] = true
			used[i] = true
		}
	}

	if !colUsed[1] {
		return nil, errors.New("table_name and before_time constraints must be supplied")
	}
	return &sqlite3.IndexResult{
		Used: used,
	}, nil
}

func (vm *VacuumModule) Open() (sqlite3.VTabCursor, error) {
	return &VacuumCursor{}, nil
}

func (vm *VacuumModule) Disconnect() error { return nil }
func (vm *VacuumModule) Destroy() error    { return nil }

type VacuumCursor struct {
	tableName  string
	beforeTime time.Time
	vacuumErr  error
	eof        bool
}

func (vc *VacuumCursor) Next() error {
	vc.eof = true
	return nil
}

func (vc *VacuumCursor) Rowid() (int64, error) {
	return 0, nil
}

func (vc *VacuumCursor) Column(context *sqlite3.SQLiteContext, i int) error {
	switch i {
	case 0:
		if vc.vacuumErr != nil {
			context.ResultText(fmt.Sprintf("%v", vc.vacuumErr))
		} else {
			context.ResultNull()
		}
	case 1:
		context.ResultText(vc.tableName)
	case 2:
		context.ResultText(vc.beforeTime.Format(s3db.SQLiteTimeFormat))
	}
	return nil
}

func (vc *VacuumCursor) EOF() bool {
	return vc.eof
}

func (vc *VacuumCursor) Close() error { return nil }

func (vc *VacuumCursor) Filter(_ int, idxStr string, values []interface{}) error {
	if len(values) != 2 {
		return errors.New("table_name and before_time constraints must be supplied")
	}
	tableName, ok := values[0].(string)
	if !ok {
		return errors.New("table_name and before_time constraints must be supplied")
	}
	tp, ok := values[1].(string)
	if !ok {
		return errors.New("table_name and before_time constraints must be supplied")
	}

	vc.tableName = tableName
	t, err := time.Parse(s3db.SQLiteTimeFormat, tp)
	if err != nil {
		return fmt.Errorf("before_time: %w (must be like %s)", err, s3db.SQLiteTimeFormat)
	}
	vc.beforeTime = t

	vc.vacuumErr = s3db.Vacuum(tableName, t)
	return nil
}
