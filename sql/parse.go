package sql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jrhy/s3db/sql/colval"
	"github.com/jrhy/s3db/sql/parse"
	"github.com/jrhy/s3db/sql/types"
)

var StringValueRE = regexp.MustCompile(`^('(([^']|'')*)'|"([^"]*)")`)
var IntValueRE = regexp.MustCompile(`^\d+`)
var RealValueRE = regexp.MustCompile(`^(((\+|-)?([0-9]+)(\.[0-9]+)?)|((\+|-)?\.?[0-9]+))([Ee]\d+)?`)

func ColumnValueParser(cv *colval.ColumnValue) parse.Func {
	return parse.OneOf(
		parse.RE(StringValueRE, func(s []string) bool {
			if len(s[2]) > 0 {
				*cv = colval.Text(strings.ReplaceAll(s[2], `''`, `'`))
			} else {
				*cv = colval.Text(s[4])
			}
			return true
		}),
		parse.RE(RealValueRE, func(s []string) bool {
			if !strings.ContainsAny(s[0], ".eE") {
				fmt.Printf("parsing as int...\n")
				i, err := strconv.ParseInt(s[0], 0, 64)
				fmt.Printf("the int: %d, conversion error: %v\n", i, err)
				if err == nil {
					*cv = colval.Int(i)
					return true
				}
				// too big or whatever, try again as real
			}
			f, err := strconv.ParseFloat(s[0], 64)
			if err != nil {
				return false
			}
			*cv = colval.Real(f)
			return true
		}),
		parse.CI("null").Action(func() {
			*cv = colval.Null{}
		}),
	)
}

func Schema(s *types.Schema, errs *[]error) parse.Func {
	return func(b *parse.Parser) bool {
		var col, coltype, name string
		return b.Match(
			parse.Delimited(
				parse.OneOf(
					parse.SeqWS(
						parse.CI("primary"), parse.CI("key"),
						parse.Exact("(").
							Action(func() {
								if len(s.PrimaryKey) > 0 {
									*errs = append(*errs, errors.New("PRIMARY KEY specified multiple times"))
								}
							}),
						parse.Delimited(
							parse.SeqWS(
								SQLName(&col).
									Action(func() {
										s.PrimaryKey = append(s.PrimaryKey, col)
									})),
							parse.Exact(",")),
						parse.Exact(")")),
					parse.SeqWS(
						SQLName(&col).
							Action(func() { s.Columns = append(s.Columns, types.SchemaColumn{Name: col}) }),
						parse.Optional(ColumnType(&coltype)).Action(func() {
							s.Columns[len(s.Columns)-1].DefaultType =
								strings.ToLower(coltype)
						}),
						parse.Multiple(
							parse.OneOf(
								parse.SeqWS(parse.CI("primary"), parse.CI("key")).
									Action(func() {
										if len(s.PrimaryKey) > 0 {
											*errs = append(*errs, errors.New("PRIMARY KEY already specified"))
										} else {
											s.PrimaryKey = []string{col}
										}
									}),
								parse.CI("unique").Action(func() {
									s.Columns[len(s.Columns)-1].Unique = true
									*errs = append(*errs, errors.New("UNIQUE is not supported yet"))
								}),
								parse.SeqWS(parse.CI("not"), parse.CI("null")).Action(func() {
									s.Columns[len(s.Columns)-1].NotNull = true
								}),
							)))),
				parse.Exact(",")).Action(func() { s.Name = name }))
	}
}

func debugHere(s string) parse.Func {
	return func(p *parse.Parser) bool {
		fmt.Printf("DBG debugHere %s, remaining: %s\n", s, p.Remaining)
		return true
	}
}

func limit(res **int64, errors *[]error) parse.Func {
	return parse.SeqWS(parse.CI("limit"), parseExpressionToInt(res, "limit", errors))
}
func offset(res **int64, errors *[]error) parse.Func {
	return parse.SeqWS(parse.CI("offset"), parseExpressionToInt(res, "offset", errors))
}
func parseExpressionToInt(res **int64, name string, errors *[]error) parse.Func {
	var e *types.Evaluator
	return func(b *parse.Parser) bool {
		return b.Match(
			Expression(&e).Action(func() {
				if len(e.Inputs) > 0 {
					fmt.Printf("inputs: %+v\n", e.Inputs)
					*errors = append(*errors, fmt.Errorf("%s: int expression cannot reference", name))
					return
				}
				v := e.Func(nil)
				switch x := v.(type) {
				case colval.Int:
					i := int64(x)
					*res = &i
				default:
					*errors = append(*errors, fmt.Errorf("%s: got %T, expected int expression", name, x))
				}
			}),
		)
	}
}

func ResolveColumnRef(s *types.Schema, name string, schema **types.Schema, columnRes **types.SchemaColumn) error {
	var resolvedSchema *types.Schema
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return fmt.Errorf("nested schema reference unimplemented: %s", name)
	}
	var column *types.SchemaColumn
	if len(parts) == 2 {
		var found bool
		s, found = s.Sources[parts[0]]
		if !found {
			return fmt.Errorf("schema not found: %s", name)
		}
		column = findColumn(s, parts[1])
	} else {
		resolvedSchema, column = resolveUnqualifiedColumnReference(s, parts[0])
		if resolvedSchema != nil {
			s = resolvedSchema
		}
	}
	if column == nil {
		return fmt.Errorf("not found: column %s, in %s", name, s.Name)
	}
	if schema != nil {
		*schema = s
	}
	if columnRes != nil {
		*columnRes = column
	}
	return nil
}

func findColumn(s *types.Schema, name string) *types.SchemaColumn {
	for _, c := range s.Columns {
		if strings.EqualFold(c.Name, name) {
			return &c
		}
	}
	return nil
}
func FindColumnIndex(s *types.Schema, name string) *int {
	for i, c := range s.Columns {
		if strings.EqualFold(c.Name, name) {
			fmt.Printf("returning MATCH column %d, c.Name=%s, name=%s\n", i, c.Name, name)
			return &i
		}
	}
	return nil
}

func resolveUnqualifiedColumnReference(s *types.Schema, name string) (*types.Schema, *types.SchemaColumn) {
	for _, c := range s.Sources {
		if res := findColumn(c, name); res != nil {
			return c, res
		}
	}
	return nil, nil
}

func where(evaluator **types.Evaluator) parse.Func {
	return func(b *parse.Parser) bool {
		return b.Match(parse.SeqWS(
			parse.CI("where"),
			Expression(evaluator),
		))
	}
}

func name(res *string) parse.Func {
	return func(b *parse.Parser) bool {
		var name string
		return b.Match(parse.SeqWS(
			SQLName(&name),
		).Action(func() { *res = name }))
	}
}

var sqlNameRE = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z_0-9-\.]*)|^('(([^']|'')*)'|"([^"]*)")`)

func SQLName(res *string) parse.Func {
	return parse.RE(sqlNameRE, func(s []string) bool {
		if len(s[1]) > 0 {
			*res = s[1]
		} else if len(s[3]) > 0 {
			*res = strings.ReplaceAll(s[3], `''`, `'`)
		} else {
			*res = s[5]
		}
		return true
	})
}

var typeRE = regexp.MustCompile(`^(?i:text|varchar|integer|number|real)`)

func ColumnType(res *string) parse.Func {
	return parse.RE(typeRE, func(s []string) bool {
		*res = s[0]
		return true
	})
}

func As(as *string) parse.Func {
	return parse.SeqWS(
		parse.Optional(parse.CI("as")),
		SQLName(as))
}

func FromItemAs(as *string) parse.Func {
	return parse.SeqWS(
		parse.Exact("").Action(func() { fmt.Printf("DBG starting fromAs") }),
		parse.Optional(parse.CI("as")),
		FromItemAlias(as))
}

var sqlKeywordRE = regexp.MustCompile(`^(?i:select|from|where|join|outer|inner|left|on|using|union|except|group|all|distinct|order|limit|offset)`)
var matchKeyword = parse.RE(sqlKeywordRE, func(_ []string) bool { return true })

func FromItemAlias(res *string) parse.Func {
	return func(p *parse.Parser) bool {
		if matchKeyword(p) {
			return false
		}
		return SQLName(res)(p)
	}
}

// TODO: consider if the constraint on 'insert or replace' or 'insert or ignore' for tables with keys may be an intuitive way to convey how coordinatorless tables will work.

func orderBy(op *[]types.OrderBy, sources map[string]types.Source) parse.Func {
	return func(b *parse.Parser) bool {
		var o types.OrderBy
		return b.Match(parse.SeqWS(
			parse.CI("order"),
			parse.CI("by"),
			parse.Delimited(
				parse.SeqWS(Expression(&o.Expression).Action(func() {
					if len(o.Expression.Inputs) == 0 {
						if col, ok := o.Expression.Func(nil).(colval.Int); ok {
							o.OutputColumn = int(col)
							o.Expression = nil
						} else {
							panic("TODO error order by with non-integer column")
						}
					} else {
						panic("TODO error expecting column number as integer")
					}
				}),
					parse.Optional(
						parse.OneOf(
							parse.CI("asc"),
							parse.CI("desc").Action(func() { o.Desc = true }),
							//TODO parse.CI("using"), operator...,
						)),
					parse.Optional(parse.SeqWS(
						parse.CI("nulls"),
						parse.OneOf(
							parse.CI("first").Action(func() { o.NullsFirst = true }),
							parse.CI("last")))),
				).Action(func() {
					*op = append(*op, o)
					o = types.OrderBy{}
				}),
				parse.Exact(","))))
	}
}
