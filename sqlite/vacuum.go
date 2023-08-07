package mod

import (
	"errors"
	"fmt"
	"time"

	"github.com/jrhy/s3db"
	"go.riyazali.net/sqlite"
)

type VacuumModule struct{}

func (c *VacuumModule) Connect(conn *sqlite.Conn, args []string,
	declare func(string) error) (sqlite.VirtualTable, error) {

	err := declare(`create table s3db_vacuum(
		vacuum_error,
		table_name HIDDEN,
		before_time HIDDEN
	)`)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	return &VacuumModule{}, nil
}

func (vm *VacuumModule) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	used := make([]*sqlite.ConstraintUsage, len(input.Constraints))
	colUsed := make(map[int]bool)
	for i, c := range input.Constraints {
		if c.ColumnIndex >= 1 && c.ColumnIndex <= 2 && c.Op == sqlite.INDEX_CONSTRAINT_EQ {
			used[i] = &sqlite.ConstraintUsage{
				ArgvIndex: c.ColumnIndex,
				Omit:      true,
			}
			colUsed[c.ColumnIndex] = true
		}
	}

	res := &sqlite.IndexInfoOutput{
		ConstraintUsage: used,
	}
	if !colUsed[1] {
		return nil, errors.New("table_name and before_time constraints must be supplied")
	}
	return res, nil
}

func (vm *VacuumModule) Open() (sqlite.VirtualCursor, error) {
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

func (vc *VacuumCursor) Column(context *sqlite.VirtualTableContext, i int) error {
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

func (vc *VacuumCursor) Eof() bool {
	return vc.eof
}

func (vc *VacuumCursor) Close() error { return nil }

func (vc *VacuumCursor) Filter(_ int, idxStr string, values ...sqlite.Value) error {
	if len(values) != 2 || values[0].IsNil() || values[1].IsNil() ||
		values[0].Text() == "" || values[1].Text() == "" {
		return errors.New("table_name and before_time constraints must be supplied")
	}

	vc.tableName = values[0].Text()
	t, err := time.Parse(s3db.SQLiteTimeFormat, values[1].Text())
	if err != nil {
		return fmt.Errorf("before_time: %w (must be like %s)", err, s3db.SQLiteTimeFormat)
	}
	vc.beforeTime = t

	vc.vacuumErr = s3db.Vacuum(vc.tableName, t)
	return nil
}
