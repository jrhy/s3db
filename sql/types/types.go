package types

import (
	"github.com/jrhy/s3db/sql/colval"
	"github.com/jrhy/s3db/sql/parse"
)

type Expression struct {
	Parser parse.Parser
	Create *Create `json:",omitempty"`
	Drop   *Drop   `json:",omitempty"`
	Insert *Insert `json:",omitempty"`
	Select *Select `json:",omitempty"`
	Errors []error
}

type Schema struct {
	Name       string             `json:",omitempty"`
	Sources    map[string]*Schema `json:",omitempty"` // functions/tables
	Columns    []SchemaColumn     `json:",omitempty"`
	PrimaryKey []string           `json:",omitempty"`
}

type SchemaColumn struct {
	Source       string             `json:",omitempty"`
	SourceColumn string             `json:",omitempty"`
	Name         string             `json:",omitempty"`
	DefaultType  string             `json:",omitempty"`
	NotNull      bool               `json:",omitempty"`
	Unique       bool               `json:",omitempty"`
	Default      colval.ColumnValue `json:",omitempty"`
}

type Select struct {
	Create      *Create            `json:",omitempty"`
	With        []With             `json:",omitempty"` // TODO generalize With,Values,Tables to FromItems
	Expressions []OutputExpression `json:",omitempty"`
	FromItems   []FromItem         `json:",omitempty"`
	Values      *Values            `json:",omitempty"`
	Where       *Evaluator         `json:",omitempty"`
	Join        []Join             `json:",omitempty"`
	SetOp       *SetOp             `json:",omitempty"`
	OrderBy     []OrderBy          `json:",omitempty"`
	Limit       *int64             `json:",omitempty"`
	Offset      *int64             `json:",omitempty"`
	Schema      Schema             `json:",omitempty"`
	Errors      []error            // schema errors
}

type JoinType int

const (
	InnerJoin = JoinType(iota)
	LeftJoin
	OuterJoin
	RightJoin
)

type Join struct {
	JoinType JoinType   `json:",omitempty"`
	Right    FromItem   `json:",omitempty"`
	Alias    string     `json:",omitempty"` // TODO: consolidate in Right.Alias
	On       *Evaluator `json:",omitempty"`
	Using    []string   `json:",omitempty"`
}

type Values struct {
	Name   string
	Rows   []Row  `json:",omitempty"`
	Schema Schema `json:",omitempty"`
	Errors []error
}

type Row []colval.ColumnValue

func (r Row) String() string {
	res := ""
	for i := range r {
		if i > 0 {
			res += " "
		}
		res += r[i].String()
	}
	return res
}

type OutputExpression struct {
	Expression SelectExpression `json:",omitempty"`
	Alias      string           `json:",omitempty"`
}

type SelectExpression struct {
	Column *Column `json:",omitempty"`
	Func   *Func   `json:",omitempty"`
}

type Column struct {
	//Family Family
	Term string `json:",omitempty"`
	All  bool   `json:",omitempty"`
}

type Func struct {
	Aggregate bool
	Name      string
	RowFunc   func(Row) colval.ColumnValue
	//Expression OutputExpression //TODO: should maybe SelectExpression+*
}

type FromItem struct {
	TableRef *TableRef `json:",omitempty"`
	Subquery *Select   `json:",omitempty"`
	Alias    string    `json:",omitempty"`
}

type TableRef struct {
	Schema string `json:",omitempty"`
	Table  string `json:",omitempty"`
}

type With struct {
	Name   string `json:",omitempty"`
	Schema Schema
	Select Select `json:",omitempty"`
	Errors []error
}

type SetOpType int

const (
	Union = SetOpType(iota)
	Intersect
	Except
)

type SetOp struct {
	Op    SetOpType `json:",omitempty"`
	All   bool      `json:",omitempty"`
	Right *Select   `json:",omitempty"`
}

type Evaluator struct {
	// TODO want functions that can return ([][]colval.ColumnValue,error)
	Func   func(map[string]colval.ColumnValue) colval.ColumnValue
	Inputs map[string]struct{}
}

type Create struct {
	Schema Schema
	Query  *Select
	Index  *Index
	View   View //TODO pointerize
	Errors []error
}

type Index struct {
	Table string
	Expr  *Evaluator
}

type View struct {
	Columns         Schema
	ReplaceIfExists bool
}

type Insert struct {
	Schema Schema
	Query  *Select
	Values *Values
	Errors []error
}

type Source struct {
	RowIterator func() RowIterator
	Schema      func() Schema
}

type RowIterator interface {
	Next() (*Row, error)
	Schema() *Schema
}

type Drop struct {
	IfExists bool
	TableRef *TableRef `json:",omitempty"`
	Errors   []error
}
type OrderBy struct {
	Expression   *Evaluator
	OutputColumn int
	Desc         bool
	NullsFirst   bool
}
