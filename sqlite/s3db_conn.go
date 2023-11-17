package mod

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.riyazali.net/sqlite"

	"github.com/jrhy/s3db"
)

type ConnModule struct {
	sc *S3DBConn
}

var _ sqlite.WriteableVirtualTable = (*ConnModule)(nil)

func (c *ConnModule) Connect(conn *sqlite.Conn, args []string,
	declare func(string) error) (sqlite.VirtualTable, error) {

	err := declare(`create table s3db_conn(
		deadline,
		write_time
	)`)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	return c, nil
}

func (vm *ConnModule) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{
		ConstraintUsage: make([]*sqlite.ConstraintUsage, len(input.Constraints)),
	}, nil
}

func (vm *ConnModule) Open() (sqlite.VirtualCursor, error) {
	return &ConnCursor{vm, false}, nil
}

func (vm *ConnModule) Disconnect() error { return nil }
func (vm *ConnModule) Destroy() error    { return nil }

type ConnCursor struct {
	vm  *ConnModule
	eof bool
}

func (vc *ConnCursor) Next() error {
	vc.eof = true
	return nil
}

func (vc *ConnCursor) Rowid() (int64, error) {
	return 0, nil
}

func (vc *ConnCursor) Column(context *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		if vc.vm.sc.deadline.IsZero() {
			context.ResultNull()
		} else {
			context.ResultText(vc.vm.sc.deadline.Format(s3db.SQLiteTimeFormat))
		}
	case 1:
		if vc.vm.sc.writeTime.IsZero() {
			context.ResultNull()
		} else {
			context.ResultText(vc.vm.sc.writeTime.Format(s3db.SQLiteTimeFormat))
		}
	default:
		context.ResultError(fmt.Errorf("unhandled column %d", i))
	}
	return nil
}

func (vc *ConnCursor) Eof() bool    { return vc.eof }
func (vc *ConnCursor) Close() error { return nil }

func (vc *ConnCursor) Filter(_ int, idxStr string, values ...sqlite.Value) error {
	return nil
}

func (c *ConnModule) Update(value sqlite.Value, values ...sqlite.Value) error {
	var err error
	if len(values) != 2 {
		return errors.New("wrong number of column values")
	}

	deadline, writeTime := values[0], values[1]

	if !deadline.NoChange() {
		if deadline.IsNil() || deadline.Text() == "" {
			c.sc.deadline = time.Time{}
			c.sc.ctx = context.Background()
		} else {
			c.sc.deadline, err = time.Parse(s3db.SQLiteTimeFormat, deadline.Text())
			if err != nil {
				// TODO: fix other time parsing error messages
				return fmt.Errorf("deadline: must be like %s", s3db.SQLiteTimeFormat)
			}
		}
	}

	if !writeTime.NoChange() {
		if writeTime.IsNil() || writeTime.Text() == "" {
			c.sc.writeTime = time.Time{}
		} else {
			c.sc.writeTime, err = time.Parse(s3db.SQLiteTimeFormat, writeTime.Text())
			if err != nil {
				return fmt.Errorf("write_time: must be like %s", s3db.SQLiteTimeFormat)
			}
		}
	}

	c.sc.txFixedWriteTime = false
	c.sc.ResetContext()

	return nil
}

func (c *ConnModule) Insert(_ ...sqlite.Value) (int64, error) {
	return 0, sqlite.SQLITE_CONSTRAINT_VTAB
}

func (c *ConnModule) Replace(old, new sqlite.Value, _ ...sqlite.Value) error {
	return sqlite.SQLITE_CONSTRAINT_VTAB
}

func (c *ConnModule) Delete(sqlite.Value) error {
	return sqlite.SQLITE_CONSTRAINT_VTAB
}
