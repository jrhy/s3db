package mod

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.riyazali.net/sqlite"

	"github.com/jrhy/s3db"
)

type RootsModule struct{}

func (c *RootsModule) Connect(conn *sqlite.Conn, args []string,
	declare func(string) error) (sqlite.VirtualTable, error) {

	err := declare(`create table s3db_roots(
		roots,
		roots_error,
		table_name
	)`)
	if err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}

	return &RootsModule{}, nil
}

func (vm *RootsModule) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	used := make([]*sqlite.ConstraintUsage, len(input.Constraints))
	colUsed := make(map[int]bool)
	for i, c := range input.Constraints {
		if c.ColumnIndex == 2 && c.Op == sqlite.INDEX_CONSTRAINT_EQ {
			used[i] = &sqlite.ConstraintUsage{
				ArgvIndex: 1,
				Omit:      true,
			}
			colUsed[c.ColumnIndex] = true
		}
	}

	res := &sqlite.IndexInfoOutput{
		ConstraintUsage: used,
	}
	if !colUsed[2] {
		return nil, errors.New("table_name constraint must be supplied")
	}
	return res, nil
}

func (vm *RootsModule) Open() (sqlite.VirtualCursor, error) {
	return &RootsCursor{}, nil
}

func (vm *RootsModule) Disconnect() error { return nil }
func (vm *RootsModule) Destroy() error    { return nil }

type RootsCursor struct {
	roots     string
	rootsErr  error
	tableName string
	eof       bool
}

func (vc *RootsCursor) Next() error {
	vc.eof = true
	return nil
}

func (vc *RootsCursor) Rowid() (int64, error) {
	return 0, nil
}

func (vc *RootsCursor) Column(context *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		if vc.roots != "" {
			context.ResultText(vc.roots)
		} else {
			context.ResultNull()
		}
	case 1:
		if vc.rootsErr != nil {
			context.ResultText(vc.rootsErr.Error())
		} else {
			context.ResultNull()
		}
	case 2:
		context.ResultText(vc.tableName)
	}
	return nil
}

func (vc *RootsCursor) Eof() bool {
	return vc.eof
}

func (vc *RootsCursor) Close() error { return nil }

func (vc *RootsCursor) Filter(_ int, idxStr string, values ...sqlite.Value) error {
	if len(values) != 1 || values[0].IsNil() || values[0].Text() == "" {
		return errors.New("table_name constraint must be supplied")
	}

	vc.tableName = values[0].Text()
	vt := s3db.GetTable(vc.tableName)
	if vt == nil {
		return fmt.Errorf("table not found: %s", vc.tableName)
	}
	var roots []string
	roots, vc.rootsErr = vt.Tree.Root.Roots()
	if vc.rootsErr != nil {
		return nil
	}
	var je []byte
	je, vc.rootsErr = json.Marshal(roots)
	if vc.rootsErr != nil {
		return nil
	}
	vc.roots = string(je)
	return nil
}
