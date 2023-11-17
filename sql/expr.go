package sql

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

	"github.com/johncgriffin/overflow"

	"github.com/jrhy/s3db/sql/colval"
	p "github.com/jrhy/s3db/sql/parse"
	"github.com/jrhy/s3db/sql/types"
)

var (
	operatorPrecedence = make(map[string]int)
	operatorMatchOrder []string
	precedence         int
)

func init() {
	nextPrecedence("or")
	nextPrecedence("and")
	nextPrecedence("not")
	nextPrecedence("||")
	nextPrecedence("=", "!=", "==", "<>")
	nextPrecedence("<", "<=", ">", ">=")
	nextPrecedence("+", "-")
	nextPrecedence("*", "/", "%")
	nextPrecedence("<<", ">>")
	nextPrecedence("|")
	nextPrecedence("&")
	assignOperatorMatchOrder()
}

func nextPrecedence(s ...string) {
	precedence = precedence + 1
	for i := range s {
		operatorPrecedence[s[i]] = precedence
	}
}

func assignOperatorMatchOrder() {
	var maxLength int
	for k := range operatorPrecedence {
		if len(k) > maxLength {
			maxLength = len(k)
		}
	}
	for l := maxLength; l > 0; l-- {
		for k := range operatorPrecedence {
			if len(k) != l {
				continue
			}
			operatorMatchOrder = append(operatorMatchOrder, k)
		}
	}
}

func Expression(res **types.Evaluator) p.Func {
	return binaryExpr(res)
}

func toInt(cv colval.ColumnValue) *int64 {
	switch v := (cv).(type) {
	case colval.Int:
		i := int64(v)
		return &i
	case colval.Real:
		i := int64(float64(v))
		return &i
	case colval.Text:
		i, _ := strconv.ParseInt(string(v), 0, 64)
		return &i
	}
	return nil
}

func toReal(cv colval.ColumnValue) *float64 {
	switch v := (cv).(type) {
	case colval.Int:
		f := float64(v)
		return &f
	case colval.Real:
		f := float64(v)
		return &f
	case colval.Text:
		matches := RealValueRE.FindStringSubmatch(string(v))
		var f float64
		if len(matches) > 0 {
			var err error
			f, err = strconv.ParseFloat(matches[1], 64)
			if err != nil {
				fmt.Printf("ERRRRRRRRR %v\n", err)
			}
		}
		return &f
	}
	return nil
}

func binaryExpr(res **types.Evaluator) p.Func {
	var cv colval.ColumnValue
	var name string
	cvParser := ColumnValueParser(&cv)
	colRefParser := SQLName(&name)
	return func(e *p.Parser) bool {
		var valStack []types.Evaluator
		var opStack []string
		var precStack []int
		var minPrecedence = 1
		for {
			name = ""
			e.SkipWS()
			unaryMinus := false
			for {
				if e.Exact("-") {
					unaryMinus = !unaryMinus
				} else if e.Exact("+") {

				} else {
					break
				}
				e.SkipWS()
			}
			if e.Exact("(") {
				var ev *types.Evaluator
				subExpressionParser := binaryExpr(&ev)
				if e.Match(subExpressionParser) && e.SkipWS() && e.Exact(")") {
					valStack = append(valStack, *ev)
				} else {
					return false
				}
			} else if e.Match(cvParser) {
				fmt.Printf("got cv: %v\n", cv)
				cv := cv
				valStack = append(valStack, types.Evaluator{Func: func(_ map[string]colval.ColumnValue) colval.ColumnValue { return cv }})
			} else if e.Match(colRefParser) {
				name := name
				valStack = append(valStack, types.Evaluator{
					Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
						fmt.Printf("deref! %s -> %v\n", name, inputs[name])
						res, ok := inputs[name]
						if !ok {
							panic(fmt.Errorf("column reference missing in inputs: %s", name))
						}
						return res
					},
					Inputs: map[string]struct{}{name: {}},
				})
			} else {
				fmt.Printf("NO EXPR MATCH\n")
				return false
			}
			if unaryMinus {
				vals := []types.Evaluator{
					{
						Func: func(_ map[string]colval.ColumnValue) colval.ColumnValue {
							return colval.Int(0)
						},
					},
					valStack[len(valStack)-1],
				}

				valStack = append(valStack[:len(valStack)-1],
					binaryArithmetic(vals,
						overflow.Sub64, func(a, b float64) float64 { return a - b }))
				unaryMinus = false
			}

			e.SkipWS()
			for {
				fmt.Printf("input: %s\n", e.Remaining)
				if false {
					fmt.Printf("valStack: ")
					for i := range valStack {
						if len(valStack[i].Inputs) == 0 {
							fmt.Printf("%v ", valStack[i].Func(nil))
						} else {
							fmt.Printf("%v ", valStack[i])
						}
					}
				}
				fmt.Printf("\nopStack: ")
				for i := range opStack {
					fmt.Printf("%s ", opStack[i])
				}
				fmt.Printf("\nprecStack: ")
				for i := range precStack {
					fmt.Printf("%d ", precStack[i])
				}
				fmt.Printf("\n")

				matchWithPrecedence := func() bool {
					for _, op := range operatorMatchOrder {
						opPrecedence := operatorPrecedence[op]
						if minPrecedence > opPrecedence {
							continue
						}
						if !e.Exact(op) {
							continue
						}
						fmt.Printf("pushing %s\n", op)
						opStack = append(opStack, op)
						precStack = append(precStack, minPrecedence)
						if opPrecedence > minPrecedence {
							fmt.Printf("upshift!\n")
						}
						minPrecedence = opPrecedence
						return true
					}
					return false
				}
				if matchWithPrecedence() {
					break
				} else if len(valStack) >= 2 {
					fmt.Printf("downshift!\n")
					op := opStack[len(opStack)-1]
					vals := valStack[len(valStack)-2:]
					valStack = valStack[:len(valStack)-len(vals)]
					minPrecedence = precStack[len(precStack)-1]
					precStack = precStack[:len(precStack)-1]
					opStack = opStack[:len(opStack)-1]
					fmt.Printf("vals: %v, op %s\n", vals, op)
					switch op {
					case "or":
						valStack = append(valStack, or(vals))
					case "and":
						valStack = append(valStack, and(vals))
					case "-":
						valStack = append(valStack,
							binaryArithmetic(vals,
								overflow.Sub64, func(a, b float64) float64 { return a - b }))
					case "+":
						valStack = append(valStack,
							binaryArithmetic(vals,
								overflow.Add64, func(a, b float64) float64 { return a + b }))
					case "*":
						valStack = append(valStack,
							binaryArithmetic(vals,
								overflow.Mul64, func(a, b float64) float64 { return a * b }))
					case "/":
						valStack = append(valStack,
							binaryArithmetic(vals,
								overflow.Div64, func(a, b float64) float64 { return a / b }))
					case "%":
						valStack = append(valStack,
							binaryArithmetic(vals,
								func(a, b int64) (int64, bool) { return a % b, true },
								func(a, b float64) float64 { return math.Remainder(a, b) + b }))
					case "!=", "<>":
						valStack = append(valStack,
							binaryComparison(vals,
								func(a, b int64) bool { return a != b },
								func(a, b float64) bool { return a != b }))
					case "<":
						valStack = append(valStack,
							binaryComparison(vals,
								func(a, b int64) bool { return a < b },
								func(a, b float64) bool { return a < b }))
					case "<=":
						valStack = append(valStack,
							binaryComparison(vals,
								func(a, b int64) bool { return a <= b },
								func(a, b float64) bool { return a <= b }))
					case ">":
						valStack = append(valStack,
							binaryComparison(vals,
								func(a, b int64) bool { return a > b },
								func(a, b float64) bool { return a > b }))
					case ">=":
						valStack = append(valStack,
							binaryComparison(vals,
								func(a, b int64) bool { return a >= b },
								func(a, b float64) bool { return a >= b }))
					case "=":
						valStack = append(valStack, equal(vals))
					case "||":
						valStack = append(valStack, concat(vals))
					default:
						panic(op)
					}
					continue
				} else if len(valStack) == 1 {
					fmt.Printf("DONE\n")
					v := valStack[0]
					*res = &v
					return true
				}
				break
			}
		}
	}
}

func requireDimensions(x, y int, cv [][]colval.ColumnValue) error {
	if len(cv) != y || y > 0 && len(cv[0]) != x {
		return fmt.Errorf("require %dx%d dimensions", x, y)
	}
	return nil
}

func requireSingle(cv [][]colval.ColumnValue) error { return requireDimensions(1, 1, cv) }

func combineInputs(evaluators []types.Evaluator) map[string]struct{} {
	combined := make(map[string]struct{}, len(evaluators)*2)
	for i := range evaluators {
		for k := range evaluators[i].Inputs {
			combined[k] = struct{}{}
		}
	}
	return combined
}

func equal(inputs []types.Evaluator) types.Evaluator {
	capture := []types.Evaluator{inputs[0], inputs[1]}
	return types.Evaluator{
		Inputs: combineInputs(capture),
		Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
			col := []colval.ColumnValue{capture[0].Func(inputs), capture[1].Func(inputs)}
			if isNull(col[0]) || isNull(col[1]) {
				return colval.Null{}
			}
			if isText(col[0]) && isText(col[1]) {
				return boolCV(col[0].(colval.Text) == col[1].(colval.Text))
			}
			if isBlob(col[0]) && isBlob(col[1]) {
				return boolCV(bytes.Equal(col[0].(colval.Blob), col[1].(colval.Blob)))
			}
			if isInt(col[0]) {
				if isInt(col[1]) {
					return boolCV(col[0].(colval.Int) == col[1].(colval.Int))
				}
				if isReal(col[1]) {
					return boolCV(float64(col[0].(colval.Int)) == float64(col[1].(colval.Real)))
				}
			}
			if isReal(col[0]) {
				if isInt(col[1]) {
					return boolCV(float64(col[0].(colval.Real)) == float64(col[1].(colval.Int)))
				}
				if isReal(col[1]) {
					return boolCV(col[0].(colval.Real) == col[1].(colval.Real))
				}
			}
			return boolCV(false)
		}}
}
func boolCV(b bool) colval.ColumnValue {
	if b {
		return colval.Int(1)
	} else {
		return colval.Int(0)
	}
}

func isNull(cv colval.ColumnValue) bool {
	_, isNull := cv.(colval.Null)
	return isNull
}
func isInt(cv colval.ColumnValue) bool {
	_, isInt := cv.(colval.Int)
	return isInt
}
func isIntText(cv colval.ColumnValue) bool {
	s, isText := cv.(colval.Text)
	return isText && IntValueRE.MatchString(string(s))
}
func intTextValue(cv colval.ColumnValue, res *int64) bool {
	s, isText := cv.(colval.Text)
	if !isText {
		return false
	}
	i, err := strconv.ParseInt(string(s), 0, 64)
	if err != nil {
		return false
	}
	*res = i
	return true
}
func isReal(cv colval.ColumnValue) bool {
	_, isReal := cv.(colval.Real)
	return isReal
}
func isRealText(cv colval.ColumnValue) bool {
	s, isText := cv.(colval.Text)
	return isText && RealValueRE.MatchString(string(s))
}
func realTextValue(cv colval.ColumnValue, res *float64) bool {
	s, isText := cv.(colval.Text)
	if !isText {
		return false
	}
	f, err := strconv.ParseFloat(string(s), 64)
	if err != nil {
		return false
	}
	*res = f
	return true
}
func isText(cv colval.ColumnValue) bool {
	_, isText := cv.(colval.Text)
	return isText
}
func isBlob(cv colval.ColumnValue) bool {
	_, isBlob := cv.(colval.Blob)
	return isBlob
}

func or(inputs []types.Evaluator) types.Evaluator {
	capture := []types.Evaluator{inputs[0], inputs[1]}
	return types.Evaluator{
		Inputs: combineInputs(capture),
		Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
			col := []colval.ColumnValue{capture[0].Func(inputs), capture[1].Func(inputs)}
			left := col[0].ToBool()
			if left != nil && *left {
				return colval.Int(1)
			}
			right := col[1].ToBool()
			if right != nil && *right {
				return colval.Int(1)
			}
			if left == nil || right == nil {
				return colval.Null{}
			}
			return colval.Int(0)
		}}
}

func and(inputs []types.Evaluator) types.Evaluator {
	capture := []types.Evaluator{inputs[0], inputs[1]}
	return types.Evaluator{
		Inputs: combineInputs(capture),
		Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
			col := []colval.ColumnValue{capture[0].Func(inputs), capture[1].Func(inputs)}
			left := col[0].ToBool()
			right := col[1].ToBool()
			if left != nil && right != nil {
				return boolCV(*left && *right)
			}
			if left != nil && !*left || right != nil && !*right {
				return colval.Int(0)
			}
			return colval.Null{}
		}}
}

func binaryArithmetic(
	inputs []types.Evaluator,
	intFunc func(int64, int64) (int64, bool),
	realFunc func(float64, float64) float64,
) types.Evaluator {
	capture := []types.Evaluator{inputs[0], inputs[1]}
	return types.Evaluator{
		Inputs: combineInputs(capture),
		Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
			col := []colval.ColumnValue{capture[0].Func(inputs), capture[1].Func(inputs)}
			if isNull(col[0]) || isNull(col[1]) {
				return colval.Null{}
			}
			if isReal(col[0]) || isRealText(col[0]) || isReal(col[1]) || isRealText(col[1]) {
				return colval.Real(realFunc(*toReal(col[0]), *toReal(col[1])))
			}
			left := *toInt(col[0])
			right := *toInt(col[1])
			res, ok := intFunc(left, right)
			if !ok {
				return colval.Real(realFunc(*toReal(col[0]), *toReal(col[1])))
			}
			return colval.Int(res)
		}}
}

/*
func binaryComparison(

	inputs []types.Evaluator,
	intFunc func(int64, int64) (bool, bool),
	realFunc func(float64, float64) bool,

	) types.Evaluator {
		arithmeticEvaluator := binaryArithmetic(inputs,
			func(a, b int64) (int64, bool) {
			if res, _ := intFunc(a,b); res { return 1, true }
			return 0, true
		}, func(a, b float64) float64 {
			if realFunc(a,b) { return 1.0 }
			return 0.0
		})
		inner := arithmeticEvaluator.Func
		arithmeticEvaluator.Func =
			func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
				cv :=  inner(inputs)
				if r, isReal := cv.(colval.Real) {
					return colval.Int(int64(r))
				}
			}

		return arithmeticEvaluator
	}
*/
func binaryComparison(
	inputs []types.Evaluator,
	intFunc func(int64, int64) bool,
	realFunc func(float64, float64) bool,
) types.Evaluator {
	capture := []types.Evaluator{inputs[0], inputs[1]}
	return types.Evaluator{
		Inputs: combineInputs(capture),
		Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
			col := []colval.ColumnValue{capture[0].Func(inputs), capture[1].Func(inputs)}
			if isNull(col[0]) || isNull(col[1]) {
				return colval.Null{}
			}
			if isReal(col[0]) || isRealText(col[0]) || isReal(col[1]) || isRealText(col[1]) {
				return boolCV(realFunc(*toReal(col[0]), *toReal(col[1])))
			}
			return boolCV(intFunc(*toInt(col[0]), *toInt(col[1])))
		}}
}

func concat(
	inputs []types.Evaluator,
) types.Evaluator {
	capture := []types.Evaluator{inputs[0], inputs[1]}
	return types.Evaluator{
		Inputs: combineInputs(capture),
		Func: func(inputs map[string]colval.ColumnValue) colval.ColumnValue {
			col := []colval.ColumnValue{capture[0].Func(inputs), capture[1].Func(inputs)}
			if isNull(col[0]) || isNull(col[1]) {
				return colval.Null{}
			}
			return colval.Text(col[0].String() + col[1].String())
		}}
}

func ColumnValueLess(a, b colval.ColumnValue) bool {
	if isNull(a) || isNull(b) {
		return true
	}
	if isReal(a) || isRealText(a) || isReal(b) || isRealText(b) {
		return *toReal(a) < *toReal(b)
	}
	return *toInt(a) < *toInt(b)

}
