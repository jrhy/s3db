package s3db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/segmentio/ksuid"

	"github.com/jrhy/mast/persist/s3test"
	"github.com/jrhy/s3db/kv"
	"github.com/jrhy/s3db/kv/crdt"
	"github.com/jrhy/s3db/sql"
	"github.com/jrhy/s3db/sql/colval"
	"github.com/jrhy/s3db/sql/parse"
	sqlTypes "github.com/jrhy/s3db/sql/types"

	"github.com/jrhy/mast"
	v1proto "github.com/jrhy/s3db/proto/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

var ErrS3DBConstraintNotNull = errors.New("constraint: NOT NULL")
var ErrS3DBConstraintPrimaryKey = errors.New("constraint: key not unique")
var ErrS3DBConstraintUnique = errors.New("constraint: not unique")

var (
	tableLock sync.Mutex
	tables    map[string]*VirtualTable
)

func init() {
	tables = make(map[string]*VirtualTable)
}

type KV struct {
	Root   *kv.DB
	closer func()
}

type Module struct{}

type VirtualTable struct {
	Name              string
	Ctx               context.Context
	cancelFunc        func()
	SchemaString      string
	schema            *sqlTypes.Schema
	ColumnIndexByName map[string]int
	ColumnNameByIndex map[int]string
	Tree              *KV
	txStart           *kv.DB
	keyCol            int
	usesRowID         bool
	writeTime         *time.Time

	s3 s3Options
}

func unquoteAll(s string) string {
	if len(s) == 0 {
		return ""
	}
	p := &parse.Parser{
		Remaining: s,
	}
	var cv colval.ColumnValue
	var res string
	for {
		if ok := sql.ColumnValueParser(&cv)(p); !ok {
			dbg("skipping unquote; using: %s\n", s)
			return s
		}
		res += cv.String()
		if len(p.Remaining) == 0 {
			break
		}
	}
	dbg("unquoted to: %s\n", res)
	return res
}

const SQLiteTimeFormat = "2006-01-02 15:04:05"

func New(args []string) (*VirtualTable, error) {
	var err error

	if len(args) == 0 || len(args[0]) == 0 {
		return nil, errors.New("missing table name")
	}

	tableName := args[0]
	args = args[1:]
	var table = &VirtualTable{
		Name: tableName,
	}

	dbg("CONNECT\n")
	table.Ctx = context.Background()
	if len(args) == 0 {
		// columns='<colname> [type] [primary key] [not null], ...',
		return nil, errors.New(`
usage:
 columns='<colname> [primary key], ...',
                                   columns and constraints
[deadline='<N>[s,m,h,d]',]         timeout operations if they take too long (defaults to forever)
[entries_per_node=<N>,]            the number of rows to store in per S3 object (defaults to 4096)
[node_cache_entries=<N>,]          number of nodes to cache in memory (defaults to 0)
[s3_bucket='mybucket',]            defaults to in-memory bucket
[s3_endpoint='https://minio.example.com',]
                                   S3 endpoint, if not using AWS
[s3_prefix='/prefix',]             separate tables within a bucket
[write_time='2006-01-02 15:04:05',]
                                   value modification time, for idempotence, from request time`)
	}
	seen := map[string]struct{}{}
	for i := range args {
		s := strings.SplitN(args[i], "=", 2)
		if _, ok := seen[s[0]]; ok {
			return nil, fmt.Errorf("duplicated: %s", s[0])
		}
		seen[s[0]] = struct{}{}
		switch s[0] {
		case "columns":
			err = convertSchema(unquoteAll(s[1]), table)
			if err != nil {
				return nil, fmt.Errorf("schema: %w", err)
			}
		case "deadline":
			d, err := time.ParseDuration(unquoteAll(s[1]))
			if err != nil {
				return nil, fmt.Errorf("deadline: %w", err)
			}
			t := time.Now().Add(d)
			table.Ctx, table.cancelFunc = context.WithDeadline(context.Background(), t)
		case "entries_per_node":
			i, err := strconv.ParseInt(s[1], 0, 32)
			if err != nil {
				return nil, fmt.Errorf("arg: %w", err)
			}
			table.s3.EntriesPerNode = int(i)
		case "node_cache_entries":
			i, err := strconv.ParseInt(s[1], 32, 0)
			if err != nil {
				return nil, fmt.Errorf("arg: %w", err)
			}
			table.s3.NodeCacheEntries = int(i)
		case "s3_bucket":
			table.s3.Bucket = unquoteAll(s[1])
		case "s3_endpoint":
			table.s3.Endpoint = unquoteAll(s[1])
		case "s3_prefix":
			table.s3.Prefix = unquoteAll(s[1])
		case "write_time":
			t, err := time.Parse(SQLiteTimeFormat, unquoteAll(s[1]))
			if err != nil {
				return nil, fmt.Errorf("write_time: %w", err)
			}
			table.writeTime = &t
		default:
			dbg("skipping arg %s\n", args[i])
		}
	}

	if table.SchemaString == "" {
		return nil, errors.New(`unspecified: columns='colname [type] [primary key] [not null], ...'`)
	}

	table.Tree, err = newKV(table.Ctx, table.s3, "s3db-rows")
	if err != nil {
		return nil, fmt.Errorf("s3db: %w", err)
	}

	tableLock.Lock()
	defer tableLock.Unlock()
	if _, ok := tables[table.Name]; ok {
		return nil, fmt.Errorf("table already exists: %s", table.Name)
	}
	tables[table.Name] = table

	return table, nil
}

func convertSchema(s string, t *VirtualTable) error {
	schema, err := parseSchema(s)
	if err != nil {
		return err
	}
	if len(schema.PrimaryKey) > 1 {
		return fmt.Errorf("sqlite vtable primary key cannot be composite")
	}
	columnMap := map[string]struct{}{}
	for i := range schema.Columns {
		name := schema.Columns[i].Name
		if _, ok := columnMap[name]; ok {
			return fmt.Errorf("duplicate column: %s", name)
		}
		columnMap[schema.Columns[i].Name] = struct{}{}
	}
	t.usesRowID = true
	var keyColName string
	if len(schema.PrimaryKey) > 0 {
		keyColName = schema.PrimaryKey[0]
		if _, ok := columnMap[schema.PrimaryKey[0]]; !ok {
			return fmt.Errorf("no column definition for key: %s", keyColName)
		}
		t.usesRowID = false
	}
	s = "CREATE TABLE x("
	if t.usesRowID {
		s += "_rowid_ HIDDEN PRIMARY KEY NOT NULL, "
	}
	for i, c := range schema.Columns {
		if i > 0 {
			s += ", "
		}
		s += c.Name
		if c.DefaultType != "" {
			s += " " + c.DefaultType
		}
		if c.Name == keyColName {
			s += " PRIMARY KEY"
			t.keyCol = i
		}
		if c.Unique && i != t.keyCol {
			return fmt.Errorf("UNIQUE not supported for non-key column: %s", c.Name)
		}
		if c.Default != nil {
			return fmt.Errorf("DEFAULT not supported: %s", c.Name)
		}
		if c.NotNull {
			s += " NOT NULL"
		}
	}
	s += ") WITHOUT ROWID"

	t.SchemaString = s
	t.schema = schema

	t.ColumnIndexByName = map[string]int{}
	t.ColumnNameByIndex = map[int]string{}
	for i, col := range t.schema.Columns {
		t.ColumnIndexByName[col.Name] = i
		t.ColumnNameByIndex[i] = col.Name
	}
	return nil
}

func parseSchema(a string) (*sqlTypes.Schema, error) {
	var schema sqlTypes.Schema
	var errs []error
	p := &parse.Parser{
		Remaining: a,
	}
	res := sql.Schema(&schema, &errs)(p)
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to parse: %+v", errs)
	}
	if !res {
		return nil, fmt.Errorf("failed to parse")
	}
	if !parse.End()(p) {
		return nil, fmt.Errorf("failed to parse at: '%s'", p.Remaining)
	}
	// b, _ := json.Marshal(schema)
	// dbg("woo, neat schema! %s\n", string(b))
	return &schema, nil
}

type Op int

const (
	OpIgnore = iota
	OpEQ
	OpLT
	OpLE
	OpGE
	OpGT
)

type IndexInput struct {
	Op          Op
	ColumnIndex int
}
type OrderInput struct {
	Column int
	Desc   bool
}
type IndexOutput struct {
	EstimatedCost  float64
	Used           []bool
	AlreadyOrdered bool
	IdxNum         int
	IdxStr         string
}

func (c *VirtualTable) BestIndex(input []IndexInput, order []OrderInput) (*IndexOutput, error) {
	cost := float64(c.Tree.Root.Size())
	out := &IndexOutput{
		EstimatedCost: cost,
		Used:          make([]bool, len(input)),
	}
	for i := range input {
		if input[i].Op == OpIgnore {
			continue
		}
		if input[i].ColumnIndex != c.keyCol {
			continue
		}
		out.Used[i] = true
		cs := strconv.FormatInt(int64(input[i].Op), 10)
		if out.IdxStr == "" {
			out.IdxStr = cs
		} else {
			out.IdxStr += "," + cs
		}
		out.EstimatedCost /= 2.0
	}
	out.AlreadyOrdered = true
	var desc *bool
	for i := range order {
		if order[i].Column != c.keyCol {
			out.AlreadyOrdered = false
		}
		if desc != nil {
			return nil, errors.New("order specified multiple times")
		}
		v := order[i].Desc
		desc = &v
	}
	if desc == nil {
		a := false
		desc = &a
	}
	if *desc {
		out.IdxStr = "desc " + out.IdxStr
	} else {
		out.IdxStr = "asc  " + out.IdxStr
	}
	dbg("BESTINDEX %+v -> %s\n", input, out.IdxStr)
	return out, nil
}

func (c *VirtualTable) Open() (*Cursor, error) {
	dbg("OPEN CURSOR\n")
	return &Cursor{
		t: c,
	}, nil
}

func (c *VirtualTable) Disconnect() error {
	dbg("DISCONNECT\n")

	c.Tree.Root.Cancel()
	if c.Tree.closer != nil {
		c.Tree.closer()
	}
	c.Tree = nil
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
	c.Ctx = nil

	tableLock.Lock()
	defer tableLock.Unlock()
	if _, ok := tables[c.Name]; !ok {
		panic("table not found")
	}
	delete(tables, c.Name)

	return nil
}

type Cursor struct {
	t          *VirtualTable
	currentKey *Key
	currentRow *v1proto.Row
	cursor     *kv.Cursor
	desc       bool
	eof        bool
	ops        []Op
	operands   []*Key
	//REMOVE rowid   int64
	min, max     *Key
	gtMin, ltMax bool
}

func (c *Cursor) Next() error {
	dbg("NEXT\n")
	if c.eof {
		dbg("NEXT EOF\n")
		return nil
	}
	for {
		k, v, ok := c.cursor.Get()
		dbg("next got: %+v %+v %+v\n", k, v, ok)
		if !ok {
			dbg("next !ok\n")
			c.eof = true
			return nil
		}
		// stop at end of range
		if !c.desc { // order asc
			if c.max != nil {
				cmp := k.(*Key).Order(c.max)
				if c.ltMax && cmp >= 0 || cmp > 0 {
					dbg("next k=%v at c.max limit=%v\n", k, c.max)
					c.eof = true
					return nil
				}
			}
		} else { // order desc
			if c.min != nil {
				cmp := k.(*Key).Order(c.min)
				if c.gtMin && cmp <= 0 || cmp < 0 {
					dbg("next at c.min limit\n")
					c.eof = true
					return nil
				}
			}
		}
		skip := false
		if !c.desc {
			if c.min != nil && c.gtMin && k.(*Key).Order(c.min) == 0 {
				dbg("next skip\n")
				skip = true
				c.gtMin = false
			}
		} else {
			if c.max != nil && c.ltMax && k.(*Key).Order(c.max) == 0 {
				dbg("next skip\n")
				skip = true
				c.ltMax = false
			}
		}
		if v.Value == nil || v.Value.(*v1proto.Row) == nil || v.Value.(*v1proto.Row).Deleted {
			dbg("next skip2\n")
			skip = true
		}
		if !c.desc {
			dbg("next: cursor.Forward()\n")
			err := c.cursor.Forward(c.t.Ctx)
			if err != nil {
				return fmt.Errorf("forward: %w", err)
			}
		} else {
			dbg("next: cursor.Backward()\n")
			err := c.cursor.Backward(c.t.Ctx)
			if err != nil {
				return fmt.Errorf("backward: %w", err)
			}
		}
		if skip {
			dbg("skip %+v\n", k)
			continue
		}
		c.currentRow = v.Value.(*v1proto.Row)
		c.currentKey = k.(*Key)
		dbg("next: returning nil, w/currentRow: %+v\n", c.currentRow)
		return nil
	}
}

func (c *Cursor) Column(i int) (interface{}, error) {
	if c.currentRow.Deleted {
		return nil, fmt.Errorf("accessing deleted row")
	}
	var res interface{}
	if i == c.t.keyCol {
		res = c.currentKey.Value()
	} else if cv, ok := c.currentRow.ColumnValues[c.t.ColumnNameByIndex[i]]; ok {
		res = fromSQLiteValue(cv.Value)
	}
	dbg("column %d: %T %+v\n", i, res, res)
	return res, nil
}

func (c *Cursor) Filter(idxStr string, val []interface{}) error {
	dbg("FILTER idxStr=%+v val=%+v\n", idxStr, val)
	c.ops = make([]Op, len(val))
	c.operands = make([]*Key, len(val))
	switch idxStr[:4] {
	case "desc":
		c.desc = true
	case "asc ":
		c.desc = false
	default:
		return errors.New("malformed filter index string: " + idxStr)
	}
	idxStr = idxStr[5:]
	c.max = nil
	c.min = nil
	for i, s := range strings.Split(idxStr, ",") {
		if s == "" {
			continue
		}
		opInt, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			res := fmt.Errorf("parse op %s: %w", s, err)
			return res
		}
		op := Op(opInt)
		c.ops[i] = op
		c.operands[i] = NewKey(val[i])
		if op == OpLT || op == OpLE || op == OpEQ {
			if c.max == nil || c.max != nil && c.operands[i].Order(c.max) < 0 {
				c.max = c.operands[i]
				c.ltMax = op == OpLT
			}
		}
		if op == OpGT || op == OpGE || op == OpEQ {
			if c.min == nil || c.min != nil && c.operands[i].Order(c.min) > 0 {
				c.min = c.operands[i]
				c.gtMin = op == OpGT
			}
		}
	}
	var err error
	c.cursor, err = c.t.Tree.Root.Cursor(c.t.Ctx)
	if err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	if !c.desc {
		if c.min != nil {
			err = c.cursor.Ceil(c.t.Ctx, c.min)
		} else {
			err = c.cursor.Min(c.t.Ctx)
		}
	} else {
		if c.max != nil {
			err = c.cursor.Ceil(c.t.Ctx, c.max)
		} else {
			err = c.cursor.Max(c.t.Ctx)
		}
	}
	if err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	c.currentKey = nil
	c.currentRow = nil
	c.eof = false
	dbg("CURSOR RESETTING, now %+v before NEXT\n", c)
	res := c.Next()
	dbg("CURSOR RESET: err=%v eof? %v\n", res, c.eof)
	return res
}

func (c *Cursor) Rowid() (int64, error) {
	return 0, errors.New("rowid: invalid for WITHOUT ROWID table")
}
func (c *Cursor) Eof() bool {
	dbg("EOF CHECK: %v\n", c.eof)
	return c.eof
}
func (c *Cursor) Close() error {
	dbg("CLOSE CURSOR\n")
	return nil
}

func getRow(c *VirtualTable, key *Key, row **v1proto.Row, rowTime *time.Time) (bool, error) {
	var crdtValue crdt.Value
	ok, err := c.Tree.Root.Get(c.Ctx, key, &crdtValue)
	if err != nil {
		return false, err
	}
	if ok {
		*row, _ = crdtValue.Value.(*v1proto.Row)
		*rowTime = time.Unix(0, crdtValue.ModEpochNanos)
	} else {
		*row = &v1proto.Row{}
	}
	return ok, nil
}

func (c *VirtualTable) Insert(values map[int]interface{}) (int64, error) {
	dbg("INSERT ")
	if _, ok := values[c.keyCol]; !ok {
		return 0, errors.New("insert without key")
	}
	t := c.updateTime()
	var key interface{}
	if c.usesRowID {
		r, err := ksuid.NewRandomWithTime(t)
		if err != nil {
			return 0, fmt.Errorf("ksuid: %w", err)
		}
		key = r.String()
	} else {
		key = values[c.keyCol]
		if key == nil {
			return 0, ErrS3DBConstraintNotNull
		}
	}
	dbg("%T %+v\n", key, key)
	var old *v1proto.Row
	var new v1proto.Row
	var ot time.Time
	ok, err := getRow(c, NewKey(key), &old, &ot)
	if err != nil {
		return 0, fmt.Errorf("get: %w", err)
	}
	if ok && (!old.Deleted || !ot.Add(old.DeleteUpdateOffset.AsDuration()).Before(t)) {
		return 0, ErrS3DBConstraintPrimaryKey
	}
	new.ColumnValues = make(map[string]*v1proto.ColumnValue)
	for i, v := range values {
		if i == c.keyCol {
			continue
		}
		colName := c.ColumnNameByIndex[i]
		new.ColumnValues[colName] = &v1proto.ColumnValue{Value: toSQLiteValue(v)}
		dbg("SET %d %v=%v\n", i, key, v)
	}
	merged := MergeRows(key, ot, old, t, &new, t)
	err = c.Tree.Root.Set(c.Ctx, t, NewKey(key), merged)
	if err != nil {
		return 0, fmt.Errorf("set: %w", err)
	}
	return 0, nil
}

func (c *VirtualTable) Update(key interface{}, values map[int]interface{}) error {
	dbg("UPDATE ")
	if key == nil {
		key = values[c.keyCol]
	}
	if key == nil {
		return errors.New("no key set")
	}
	t := c.updateTime()
	var old *v1proto.Row
	var new v1proto.Row
	var ot time.Time
	ok, err := getRow(c, NewKey(key), &old, &ot)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if !ok || old.Deleted {
		return nil
	}
	new.ColumnValues = make(map[string]*v1proto.ColumnValue)
	for i, v := range values {
		if i == c.keyCol {
			continue
		}
		dbg("SET %d %v=%v\n", i, key, v)
		colName := c.ColumnNameByIndex[i]
		new.ColumnValues[colName] = ToColumnValue(v)
	}
	merged := MergeRows(key, ot, old, t, &new, t)
	err = c.Tree.Root.Set(c.Ctx, t, NewKey(key), merged)
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}
	return nil
}

func (c *VirtualTable) updateTime() time.Time {
	if c.writeTime != nil {
		return *c.writeTime
	}
	return time.Now()
}

func (c *VirtualTable) Delete(key interface{}) error {
	dbg("DELETE ")
	dbg("nochange=%v %s %+v\n", key, key, key)
	var old *v1proto.Row
	var new v1proto.Row
	var ot time.Time
	_, err := getRow(c, NewKey(key), &old, &ot)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	t := c.updateTime()
	new.Deleted = true
	merged := MergeRows(key, ot, old, t, &new, t)
	err = c.Tree.Root.Set(c.Ctx, t, NewKey(key), merged)
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}
	return nil
}

type s3Options struct {
	Bucket   string
	Endpoint string
	Prefix   string

	EntriesPerNode   int
	NodeCacheEntries int
}

func getS3(endpoint string) (*s3.S3, error) {
	config := aws.Config{}
	if endpoint != "" {
		config.Endpoint = &endpoint
		config.S3ForcePathStyle = aws.Bool(true)
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		return nil, fmt.Errorf("session: %w", err)
	}

	return s3.New(sess), nil
}

func newKV(ctx context.Context, s3opts s3Options, subdir string) (*KV, error) {
	var err error
	var c kv.S3Interface
	var closer func()
	if (s3opts == s3Options{}) {
		var bucketName string
		var s3Client *s3.S3
		s3Client, bucketName, closer = s3test.Client()
		s3opts = s3Options{
			Endpoint: s3Client.Endpoint,
			Bucket:   bucketName,
		}
		c = s3Client
	} else {
		c, err = getS3(s3opts.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("s3 client: %w", err)
		}
	}
	path := strings.TrimPrefix(strings.TrimSuffix(s3opts.Prefix, "/"), "/") + "/" + strings.TrimPrefix(subdir, "/")
	// TODO enable to observe kv bug around delete/merge needing squishing
	if false && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	cfg := kv.Config{
		Storage: &kv.S3BucketInfo{
			EndpointURL: s3opts.Endpoint,
			BucketName:  s3opts.Bucket,
			Prefix:      path,
		},
		KeysLike:                     &Key{},
		ValuesLike:                   &v1proto.Row{},
		CustomMerge:                  mergeValues,
		CustomMarshal:                marshalProto,
		CustomUnmarshal:              unmarshalProto,
		MastNodeFormat:               string(mast.V1Marshaler),
		UnmarshalUsesRegisteredTypes: true,
	}
	if s3opts.NodeCacheEntries > 0 {
		cfg.NodeCache = mast.NewNodeCache(s3opts.NodeCacheEntries)
	}
	if s3opts.EntriesPerNode > 0 {
		cfg.BranchFactor = uint(s3opts.EntriesPerNode)
	}
	s, err := kv.Open(ctx, c, cfg, kv.OpenOptions{}, time.Now())
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	dbg("%s size:%d\n", subdir, s.Size())
	return &KV{
		Root:   s,
		closer: closer,
	}, nil
}

// REMOVE var maxTime = time.Unix(1<<63-62135596801, 999999999)

func mergeValues(_ interface{}, i1, i2 crdt.Value) crdt.Value {
	if i1.Tombstoned() || i2.Tombstoned() {
		panic("not expecting tombstones")
	}
	resp := crdt.LastWriteWins(&i1, &i2)
	res := *resp
	if i1.ModEpochNanos < i2.ModEpochNanos {
		res.Value = MergeRows(nil,
			time.Unix(0, i1.ModEpochNanos), i1.Value.(*v1proto.Row),
			time.Unix(0, i2.ModEpochNanos), i2.Value.(*v1proto.Row),
			time.Unix(0, i2.ModEpochNanos),
		)
	} else {
		res.Value = MergeRows(nil,
			time.Unix(0, i2.ModEpochNanos), i2.Value.(*v1proto.Row),
			time.Unix(0, i1.ModEpochNanos), i1.Value.(*v1proto.Row),
			time.Unix(0, i1.ModEpochNanos),
		)
	}
	return res
}

func MergeRows(_ interface{},
	t1 time.Time, r1 *v1proto.Row,
	t2 time.Time, r2 *v1proto.Row,
	outTime time.Time,
) *v1proto.Row {
	res := v1proto.Row{
		ColumnValues: map[string]*v1proto.ColumnValue{},
	}
	var resetValuesBefore time.Time
	if !t1.Add(r1.DeleteUpdateOffset.AsDuration()).After(t2.Add(r2.DeleteUpdateOffset.AsDuration())) {
		res.Deleted = r2.Deleted
		res.DeleteUpdateOffset = durationpb.New(t2.Add(r2.DeleteUpdateOffset.AsDuration()).Sub(outTime))
		if r1.Deleted {
			if !r2.Deleted {
				resetValuesBefore = t2.Add(r2.DeleteUpdateOffset.AsDuration())
			}
		}
	} else {
		res.Deleted = r1.Deleted
		res.DeleteUpdateOffset = durationpb.New(t1.Add(r1.DeleteUpdateOffset.AsDuration()).Sub(outTime))
		if !r1.Deleted {
			if r2.Deleted {
				resetValuesBefore = t1.Add(r1.DeleteUpdateOffset.AsDuration())
			}
		}
	}

	if res.Deleted {
		return &res
	}

	allKeys := make(map[string]struct{})
	for k := range r1.ColumnValues {
		allKeys[k] = struct{}{}
	}
	for k := range r2.ColumnValues {
		allKeys[k] = struct{}{}
	}

	for k := range allKeys {
		v1, inR1 := r1.ColumnValues[k]
		v2, inR2 := r2.ColumnValues[k]
		switch {
		case !inR1:
			if !hideDeletedValue(t2, v2, resetValuesBefore) {
				res.ColumnValues[k] = adj(t2, v2, outTime)
			}
		case !inR2:
			if !hideDeletedValue(t1, v1, resetValuesBefore) {
				res.ColumnValues[k] = adj(t1, v1, outTime)
			}
		case UpdateTime(t1, v1).Before(UpdateTime(t2, v2)):
			if !hideDeletedValue(t2, v2, resetValuesBefore) {
				res.ColumnValues[k] = adj(t2, v2, outTime)
			}
		default:
			if !hideDeletedValue(t1, v1, resetValuesBefore) {
				res.ColumnValues[k] = adj(t1, v1, outTime)
			}
		}
	}
	return &res
}

func hideDeletedValue(inputTime time.Time, cv *v1proto.ColumnValue, resetValuesBefore time.Time) bool {
	return UpdateTime(inputTime, cv).Before(resetValuesBefore)
}

func adj(inTime time.Time, cv *v1proto.ColumnValue, outTime time.Time) *v1proto.ColumnValue {
	if inTime.Equal(outTime) {
		return cv
	}
	out := proto.Clone(cv).(*v1proto.ColumnValue)
	out.UpdateOffset = durationpb.New(UpdateTime(inTime, cv).Sub(outTime))
	return out
}

func (c *VirtualTable) Begin() error {
	dbg("BEGIN\n")
	var err error
	if c.txStart != nil {
		panic("transaction already in progress")
	}
	c.txStart, err = c.Tree.Root.Clone(c.Ctx)
	if err != nil {
		return fmt.Errorf("clone: %w", err)
	}
	return nil
}

func (c *VirtualTable) Commit() error {
	dbg("COMMIT\n")
	_, err := c.Tree.Root.Commit(c.Ctx)
	if err != nil {
		return fmt.Errorf("commit tree: %w", err)
	}
	c.txStart = nil
	return nil
}

func (c *VirtualTable) Rollback() error {
	dbg("ROLLBACK\n")
	c.Tree.Root.Cancel()
	c.Tree.Root = c.txStart
	c.txStart = nil
	return nil
}

func dbg(f string, v ...interface{}) {
	if false {
		fmt.Printf(f, v...)
	}
}

func marshalProto(i interface{}) ([]byte, error) {
	in := i.(mast.Node)
	out := v1proto.Node{
		Key:   make([]*v1proto.SQLiteValue, len(in.Key)),
		Value: make([]*v1proto.CRDTValue, len(in.Value)),
		Link:  make([]string, len(in.Link)),
	}
	for i := range in.Key {
		out.Key[i] = in.Key[i].(*Key).SQLiteValue
	}
	for i := range in.Value {
		row, _ := in.Value[i].(crdt.Value).Value.(*v1proto.Row)
		out.Value[i] = &v1proto.CRDTValue{
			ModEpochNanos:            in.Value[i].(crdt.Value).ModEpochNanos,
			TombstoneSinceEpochNanos: in.Value[i].(crdt.Value).TombstoneSinceEpochNanos,
			PreviousRoot:             in.Value[i].(crdt.Value).PreviousRoot,
			Value:                    row,
		}
	}
	for i := range in.Link {
		if in.Link[i] == nil {
			continue
		}
		out.Link[i] = in.Link[i].(string)
	}
	return proto.Marshal(&out)
}

func unmarshalProto(inBytes []byte, outi interface{}) error {
	var in v1proto.Node
	err := proto.Unmarshal(inBytes, &in)
	if err != nil {
		return fmt.Errorf("proto: %w", err)
	}
	out := outi.(*mast.Node)
	*out = mast.Node{
		Key:   make([]interface{}, len(in.Key)),
		Value: make([]interface{}, len(in.Value)),
		Link:  make([]interface{}, len(in.Link)),
	}
	for i := range in.Key {
		out.Key[i] = &Key{in.Key[i]}
	}
	for i := range in.Value {
		out.Value[i] = crdt.Value{
			ModEpochNanos:            in.Value[i].ModEpochNanos,
			PreviousRoot:             in.Value[i].PreviousRoot,
			TombstoneSinceEpochNanos: in.Value[i].TombstoneSinceEpochNanos,
			Value:                    in.Value[i].Value,
		}
	}
	for i := range in.Link {
		out.Link[i] = in.Link[i]
	}
	return nil
}

func fromSQLiteValue(s *v1proto.SQLiteValue) interface{} {
	switch s.Type {
	case v1proto.Type_INT:
		return s.Int
	case v1proto.Type_REAL:
		return s.Real
	case v1proto.Type_TEXT:
		return s.Text
	case v1proto.Type_BLOB:
		return s.Blob
	}
	return nil
}

func toSQLiteValue(i interface{}) *v1proto.SQLiteValue {
	return NewKey(i).SQLiteValue
}

func ToColumnValue(i interface{}) *v1proto.ColumnValue {
	return &v1proto.ColumnValue{
		Value: toSQLiteValue(i),
	}
}

func GetTable(name string) *VirtualTable {
	tableLock.Lock()
	defer tableLock.Unlock()
	return tables[name]
}

func Vacuum(tableName string, beforeTime time.Time) error {
	table := GetTable(tableName)
	if table == nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	db, err := table.Tree.Root.Clone(table.Ctx)
	if err != nil {
		return fmt.Errorf("clone: %w", err)
	}
	defer func() {
		if db != nil {
			db.Cancel()
		}
	}()
	tc, err := db.Cursor(table.Ctx)
	if err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	err = tc.Min(table.Ctx)
	if err != nil {
		return fmt.Errorf("min: %w", err)
	}
	for {
		k, v, ok := tc.Get()
		if !ok {
			break
		}
		if !v.Tombstoned() {
			row := v.Value.(*v1proto.Row)
			rowTime := time.Unix(0, v.ModEpochNanos)
			if row.Deleted && rowTime.Add(row.DeleteUpdateOffset.AsDuration()).Before(beforeTime) {
				err = db.Tombstone(table.Ctx, time.Time{}, k)
				if err != nil {
					return fmt.Errorf("tombstone %v: %w", k, err)
				}
			}
		}
		err = tc.Forward(table.Ctx)
		if err != nil {
			return fmt.Errorf("cursor forward: %w", err)
		}
	}
	err = db.RemoveTombstones(table.Ctx, beforeTime)
	if err != nil {
		return fmt.Errorf("s3db commit tombstones: %w", err)
	}
	_, err = db.Commit(table.Ctx)
	if err != nil {
		return fmt.Errorf("s3db commit tombstones: %w", err)
	}
	table.Tree.Root = db
	db = nil

	err = kv.DeleteHistoricVersions(table.Ctx, table.Tree.Root, beforeTime)
	if err != nil {
		return fmt.Errorf("s3db vacuum: %w", err)
	}

	return nil
}
