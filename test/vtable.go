package test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jrhy/s3db"
	v1proto "github.com/jrhy/s3db/proto/v1"
)

type DBOpener interface {
	OpenDB() (*sql.DB, string, string)
}

func mustJSON(i interface{}) string {
	var b []byte
	var err error
	b, err = json.Marshal(i)
	if err != nil {
		panic(err)
	}
	if len(b) > 60 {
		b, err = json.MarshalIndent(i, " ", " ")
		if err != nil {
			panic(err)
		}
	}
	return string(b)
}

func mustGetRows(r *sql.Rows) [][]interface{} {
	cols, err := r.Columns()
	if err != nil {
		panic(err)
	}
	var rows [][]interface{}
	for r.Next() {
		row := make([]interface{}, len(cols))
		for i := range row {
			var q interface{}
			row[i] = &q
		}
		err = r.Scan(row...)
		if err != nil {
			panic(fmt.Errorf("scan: %w", err))
		}
		rows = append(rows, row)
	}
	return rows
}

func expand(row []interface{}) []interface{} {
	res := []interface{}{}
	for i := range row {
		for _, v := range row[i].([]interface{}) {
			res = append(res, v)
		}
	}
	return res
}
func populateTwoTables(db *sql.DB, s3Bucket, s3Endpoint, tablePrefix, schema string, row ...interface{}) (string, string, error) {

	regTableName := tablePrefix + "_reg"
	virtualTableName := tablePrefix + "_virtual"
	_, err := db.Exec(fmt.Sprintf(`create table %s(%s)`,
		regTableName, schema))
	if err != nil {
		return "", "", fmt.Errorf("create %s: %w", regTableName, err)
	}

	_, err = db.Exec(fmt.Sprintf(`create virtual table %s using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='%s',
columns='%s')`, virtualTableName,
		s3Bucket, s3Endpoint, virtualTableName, schema))
	if err != nil {
		return "", "", fmt.Errorf("create %s: %w", virtualTableName, err)
	}

	valuesStr := "values"
	for i := range row {
		if i > 0 {
			valuesStr += ","
		}
		valuesStr += "("
		for j := range row[i].([]interface{}) {
			if j > 0 {
				valuesStr += ","
			}
			valuesStr += "?"
		}
		valuesStr += ")"
	}
	_, err = db.Exec(fmt.Sprintf("insert into %s %s", regTableName, valuesStr), expand(row)...)
	if err != nil {
		return "", "", fmt.Errorf("insert %s: %w", regTableName, err)
	}
	_, err = db.Exec(fmt.Sprintf("insert into %s %s", virtualTableName, valuesStr), expand(row)...)
	if err != nil {
		return "", "", fmt.Errorf("insert %s: %w", virtualTableName, err)
	}

	return regTableName, virtualTableName, nil
}

func row(cols ...interface{}) interface{} {
	return interface{}(cols)
}

func requireSelectEquiv(t *testing.T, db *sql.DB, regTable, virtualTable, where, expectedJSON string) {
	require.Equal(t,
		expectedJSON,
		mustQueryToJSON(db, fmt.Sprintf("select * from %s %s", regTable, where)))
	require.Equal(t,
		expectedJSON,
		mustQueryToJSON(db, fmt.Sprintf("select * from %s %s", virtualTable, where)))
}

func dump(rows *sql.Rows) error {
	colNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}
	cols := make([]interface{}, len(colNames))
	for i := range colNames {
		var ef interface{}
		cols[i] = &ef
	}
	for rows.Next() {
		err = rows.Scan(cols...)
		if err != nil {
			return err
		}
		s := "ROW: "
		for i := range cols {
			s += fmt.Sprintf("%+v ", *cols[i].(*interface{}))
		}
		fmt.Println(s)
	}
	return nil
}

func mustQueryToJSON(db *sql.DB, query string) string {
	rows, err := db.Query(query)
	if err != nil {
		panic(fmt.Errorf("%s: %w", query, err))
	}
	defer rows.Close()
	return mustJSON(mustGetRows(rows))
}

/*
// possible discrepancy to sqlite / perhaps bug in go-sqlite3
func TODOTestEmptyText(t *testing.T) {
	db, s3Bucket, s3Endpoint := getDBBucket()
	_, err := db.Exec(fmt.Sprintf(`create virtual table emptytextval using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='emptytextval',
columns='a')`,
		s3Bucket, s3Endpoint))
	require.NoError(t, err)
	_, err = db.Exec(`insert into emptytextval values ('');`)
	require.NoError(t, err)
	require.Equal(t,
		`[["text",""]]`,
		mustQueryToJSON(db, `select typeof(a), a from emptytextval`))
}

// possible discrepancy to sqlite / perhaps bug in go-sqlite3
func TODOTestEmptyBlob(t *testing.T) {
	db, s3Bucket, s3Endpoint := getDBBucket()
	_, err := db.Exec(fmt.Sprintf(`create virtual table emptyblobval using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='emptyblobval',
columns='a')`,
		s3Bucket, s3Endpoint))
	require.NoError(t, err)
	_, err = db.Exec(`insert into emptyblobval values (x'');`)
	require.NoError(t, err)
	require.Equal(t,
		`[["blob",""]]`,
		mustQueryToJSON(db, `select typeof(a), a from emptyblobval`))
}
*/

func mustParseTime(f, s string) time.Time {
	t, err := time.Parse(f, s)
	if err != nil {
		panic(err)
	}
	return t
}

func Test(t *testing.T, opener DBOpener) {

	t.Run("1", func(t *testing.T) {
		fmt.Printf("opening...\n")
		db, s3Bucket, s3Endpoint := opener.OpenDB()
		fmt.Printf("deferring...\n")
		defer db.Close()
		fmt.Printf("exec'ing...\n")
		_, err := db.Exec(fmt.Sprintf(`create virtual table t1 using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='t1',
columns='a primary key, b')`,
			s3Bucket, s3Endpoint))
		require.NoError(t, err)
		_, err = db.Exec("delete from t1;")
		require.NoError(t, err)
		_, err = db.Exec("insert into t1 values ('v1','v1b'),('v2','v2b'),('v3','v3b');")
		require.NoError(t, err)
		//_, err = db.Exec("update t1 set a='v1c' where b='v1b';")
		//require.NoError(t, err)
		_, err = db.Exec("delete from t1 where b='v2b';")
		require.NoError(t, err)
		require.Equal(t,
			`[["v1","v1b"],`+
				`["v3","v3b"]]`,
			mustQueryToJSON(db, "select * from t1;"))
	})

	t.Run("BestIndex",
		func(t *testing.T) {
			db, s3Bucket, s3Endpoint := opener.OpenDB()
			defer db.Close()
			regTable, sasqTable, err := populateTwoTables(db, s3Bucket, s3Endpoint,
				"index", "a primary key",
				row(1),
				row(3),
				row(2),
			)
			require.NoError(t, err)
			t.Run("s3dbKeySorted", func(t *testing.T) {
				require.Equal(t,
					"[[1],[2],[3]]",
					mustQueryToJSON(db, fmt.Sprintf(`select a from %s`, sasqTable)))
			})
			sqliteEquiv := func(where, expectedJSON string) func(*testing.T) {
				return func(t *testing.T) {
					requireSelectEquiv(t, db, regTable, sasqTable,
						where, expectedJSON)
				}
			}
			t.Run("gt_asc", sqliteEquiv("where a>2", "[[3]]"))
			t.Run("ge_asc", sqliteEquiv("where a>=2", "[[2],[3]]"))
			t.Run("eq_asc", sqliteEquiv("where a=2", "[[2]]"))
			t.Run("le_asc", sqliteEquiv("where a<=2", "[[1],[2]]"))
			t.Run("lt_asc", sqliteEquiv("where a<2", "[[1]]"))

			t.Run("gt_desc", sqliteEquiv("where a>2 order by 1 desc", "[[3]]"))
			t.Run("ge_desc", sqliteEquiv("where a>=2 order by 1 desc", "[[3],[2]]"))
			t.Run("eq_desc", sqliteEquiv("where a=2 order by 1 desc", "[[2]]"))
			t.Run("le_desc", sqliteEquiv("where a<=2 order by 1 desc", "[[2],[1]]"))
			t.Run("lt_desc", sqliteEquiv("where a<2 order by 1 desc", "[[1]]"))

			t.Run("and_asc", sqliteEquiv("where a>1 and a<3", "[[2]]"))

			t.Run("in_asc", sqliteEquiv("where a in (1,3)", "[[1],[3]]"))
			t.Run("in_desc", sqliteEquiv("where a in (1,3) order by 1 desc", "[[3],[1]]"))
		})

	t.Run("SortOrder",

		func(t *testing.T) {
			db, s3Bucket, s3Endpoint := opener.OpenDB()
			defer db.Close()
			_, err := db.Exec(fmt.Sprintf(`create virtual table sortfun using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='sortfun',
columns='a primary key')`,
				s3Bucket, s3Endpoint))
			require.NoError(t, err)
			_, err = db.Exec(`insert into sortfun values (?), (?), (?), (?)`,
				[]byte("blob"),
				"text",
				3.14,
				3,
			)
			require.NoError(t, err)
			require.Equal(t,
				`[["integer"],["real"],["text"],["blob"]]`,
				mustQueryToJSON(db, `select typeof(a) from sortfun`))

			_, err = db.Exec(`create table sortfun_native(a primary key) without rowid`)
			require.NoError(t, err)
			_, err = db.Exec(`insert into sortfun_native values (?), (?), (?), (?)`,
				[]byte("blob"),
				"text",
				3.14,
				3,
			)
			require.NoError(t, err)
			require.Equal(t,
				`[["integer"],["real"],["text"],["blob"]]`,
				mustQueryToJSON(db, `select typeof(a) from sortfun_native`))
		})

	t.Run("NullPrimaryKey",
		func(t *testing.T) {
			db, s3Bucket, s3Endpoint := opener.OpenDB()
			defer db.Close()
			_, err := db.Exec(fmt.Sprintf(`create virtual table nullkey using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='nullkey',
columns='a primary key')`,
				s3Bucket, s3Endpoint))
			require.NoError(t, err)
			_, err = db.Exec(`insert into nullkey values (null);`)
			require.Error(t, err, "constraint failed")
		})

	t.Run("NullValue",
		func(t *testing.T) {
			db, s3Bucket, s3Endpoint := opener.OpenDB()
			_, err := db.Exec(fmt.Sprintf(`create virtual table nullval using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='nullval',
columns='a')`,
				s3Bucket, s3Endpoint))
			require.NoError(t, err)
			_, err = db.Exec(`insert into nullval values (null);`)
			require.NoError(t, err)
			require.Equal(t,
				`[["null",null]]`,
				mustQueryToJSON(db, `select typeof(a), a from nullval`))
		})

	t.Run("ZeroInt", func(t *testing.T) {
		db, s3Bucket, s3Endpoint := opener.OpenDB()
		_, err := db.Exec(fmt.Sprintf(`create virtual table zerointval using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='zerointval',
columns='a')`,
			s3Bucket, s3Endpoint))
		require.NoError(t, err)
		_, err = db.Exec(`insert into zerointval values (0);`)
		require.NoError(t, err)
		require.Equal(t,
			`[["integer",0]]`,
			mustQueryToJSON(db, `select typeof(a), a from zerointval`))
	})

	t.Run("ZeroReal", func(t *testing.T) {
		db, s3Bucket, s3Endpoint := opener.OpenDB()
		_, err := db.Exec(fmt.Sprintf(`create virtual table zerorealval using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='zerorealval',
columns='a')`,
			s3Bucket, s3Endpoint))
		require.NoError(t, err)
		_, err = db.Exec(`insert into zerorealval values (0.0);`)
		require.NoError(t, err)
		require.Equal(t,
			`[["real",0]]`,
			mustQueryToJSON(db, `select typeof(a), a from zerorealval`))
	})

	t.Run("InsertConflict", func(t *testing.T) {
		db, s3Bucket, s3Endpoint := opener.OpenDB()
		defer db.Close()

		openTableWithWriteTime := func(t *testing.T, name, tm string) {
			mustParseTime(s3db.SQLiteTimeFormat, tm)
			stmt := fmt.Sprintf(`create virtual table "%s" using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='%s',
columns='a primary key, b',
write_time='%s')`,
				name, s3Bucket, s3Endpoint, t.Name(), tm)
			_, err := db.Exec(stmt)
			require.NoError(t, err)
		}

		for i, openerWithLatestWriteTime := range []func(*testing.T) string{
			func(t *testing.T) (expectedWinner string) {
				openTableWithWriteTime(t, t.Name()+"1", "2006-01-01 00:00:00")
				openTableWithWriteTime(t, t.Name()+"2", "2007-01-01 00:00:00")
				return "two"
			},
			func(t *testing.T) (expectedWinner string) {
				openTableWithWriteTime(t, t.Name()+"2", "2006-01-01 00:00:00")
				openTableWithWriteTime(t, t.Name()+"1", "2007-01-01 00:00:00")
				return "one"
			},
		} {

			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				expectedWinner := openerWithLatestWriteTime(t)

				_, err := db.Exec(fmt.Sprintf(`insert into "%s" values('row','one')`, t.Name()+"1"))
				require.NoError(t, err)
				_, err = db.Exec(fmt.Sprintf(`insert into "%s" values('row','two')`, t.Name()+"2"))
				require.NoError(t, err)

				openTableWithWriteTime(t, t.Name()+"read", "2008-01-01 00:00:00")
				query := fmt.Sprintf(`select * from "%s"`, t.Name()+"read")
				require.Equal(t,
					fmt.Sprintf(`[["row","%s"]]`, expectedWinner),
					mustQueryToJSON(db, query))
			})
		}
	})

	t.Run("MergeValues",
		func(t *testing.T) {
			t1 := mustParseTime(s3db.SQLiteTimeFormat, "2006-01-01 00:00:00")
			t2 := mustParseTime(s3db.SQLiteTimeFormat, "2007-01-01 00:00:00")
			v1 := &v1proto.Row{
				ColumnValues: map[string]*v1proto.ColumnValue{"b": s3db.ToColumnValue("one")},
			}
			v2 := &v1proto.Row{
				ColumnValues: map[string]*v1proto.ColumnValue{"b": s3db.ToColumnValue("two")},
			}
			res := s3db.MergeRows(nil, t1, v1, t2, v2, t2)
			require.Equal(t, time.Duration(0), res.DeleteUpdateOffset.AsDuration())
			require.Equal(t, time.Duration(0), res.ColumnValues["b"].UpdateOffset.AsDuration())
			res = s3db.MergeRows(nil, t2, v2, t1, v1, t1)
			require.Equal(t, t2.Sub(t1), res.DeleteUpdateOffset.AsDuration())
			require.Equal(t, t2.Sub(t1), res.ColumnValues["b"].UpdateOffset.AsDuration())
			res = s3db.MergeRows(nil, t2, v2, t1, v1, t2)
			require.Equal(t, time.Duration(0), res.DeleteUpdateOffset.AsDuration())
			require.Equal(t, time.Duration(0), res.ColumnValues["b"].UpdateOffset.AsDuration())
			res = s3db.MergeRows(nil, t1, v1, t2, v2, t1)
			require.Equal(t, t2.Sub(t1), res.DeleteUpdateOffset.AsDuration())
			require.Equal(t, t2.Sub(t1), res.ColumnValues["b"].UpdateOffset.AsDuration())
		})

	t.Run("Vacuum",
		func(t *testing.T) {
			db, s3Bucket, s3Endpoint := opener.OpenDB()
			defer db.Close()

			openTableWithWriteTime := func(t *testing.T, name, tm string) {
				mustParseTime(s3db.SQLiteTimeFormat, tm)
				stmt := fmt.Sprintf(`create virtual table "%s" using s3db (
s3_bucket='%s',
s3_endpoint='%s',
s3_prefix='%s',
columns='a primary key, b',
write_time='%s')`,
					name, s3Bucket, s3Endpoint, t.Name(), tm)
				_, err := db.Exec(stmt)
				require.NoError(t, err)
			}

			openTableWithWriteTime(t, "v1", "2006-01-01 00:00:00")
			_, err := db.Exec(`insert into v1 values(2006,0)`)
			require.NoError(t, err)
			_, err = db.Exec(`delete from v1`)
			require.NoError(t, err)
			require.Equal(t, `null`, mustQueryToJSON(db, `select * from v1`))

			openTableWithWriteTime(t, "v2", "2007-01-01 00:00:00")
			_, err = db.Exec(`insert into v2 values(2007,0)`)
			require.NoError(t, err)
			require.Equal(t, `[[2007,0]]`, mustQueryToJSON(db, `select * from v2`))

			openTableWithWriteTime(t, "vacuumv1", "2008-01-01 00:00:00")
			require.Equal(t,
				`[[null]]`,
				mustQueryToJSON(db, "select * from s3db_vacuum('vacuumv1','2007-01-01 00:00:00');"))
			openTableWithWriteTime(t, "readv2", "2009-01-01 00:00:00")
			require.Equal(t, `[[2007,0]]`, mustQueryToJSON(db, `select * from readv2`))
		})

	t.Run("NoPrefix",
		func(t *testing.T) {
			db, s3Bucket, s3Endpoint := opener.OpenDB()
			defer db.Close()

			_, err := db.Exec(fmt.Sprintf(`create virtual table v1noprefix using s3db (
s3_bucket='%s',
s3_endpoint='%s',
columns='a primary key, b')`,
				s3Bucket, s3Endpoint))
			require.NoError(t, err)
			_, err = db.Exec(fmt.Sprintf(`create virtual table v2noprefix using s3db (
s3_bucket='%s',
s3_endpoint='%s',
columns='a primary key, b')`,
				s3Bucket, s3Endpoint))
			require.NoError(t, err)

			_, err = db.Exec(`insert into v1noprefix values($1,$2)`, 1, 1)
			require.NoError(t, err)
			_, err = db.Exec(`insert into v2noprefix values($1,$2)`, 2, 2)
			require.NoError(t, err)

			_, err = db.Exec(fmt.Sprintf(`create virtual table noprefix using s3db (
readonly,
s3_bucket='%s',
s3_endpoint='%s',
columns='a primary key, b')`,
				s3Bucket, s3Endpoint))
			require.NoError(t, err)

			require.Equal(t,
				`[[1,1],[2,2]]`,
				mustQueryToJSON(db, "select * from noprefix;"))
		})

}
