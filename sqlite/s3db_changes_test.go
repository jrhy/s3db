package mod_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChanges_HappyCase(t *testing.T) {
	db, s3Bucket, s3Endpoint := openDB()
	defer db.Close()

	_, err := db.Exec(fmt.Sprintf(`create virtual table data using s3db (
s3_prefix='changes_happy',
s3_bucket='%s',
s3_endpoint='%s',
columns='a, b')`,
		s3Bucket, s3Endpoint))
	require.NoError(t, err)

	_, err = db.Exec(`insert into data values($1,$2)`, 1, 1)
	require.NoError(t, err)

	v1 := mustQueryVersion(db, "data")

	_, err = db.Exec(`insert into data values($1,$2)`, 2, 2)
	require.NoError(t, err)

	v2 := mustQueryVersion(db, "data")

	_, err = db.Exec(fmt.Sprintf(`create virtual table changes using s3db_changes (
table='data',
		from='%s',
		to='%s')`, v1, v2))
	require.NoError(t, err)

	require.Equal(t, `[[2,2]]`, mustQueryToJSON(db, `select * from changes`))
}

func mustQueryVersion(db *sql.DB, table string) string {
	v, err := queryVersion(db, table)
	if err != nil {
		panic(err)
	}
	return v
}

func queryVersion(db *sql.DB, table string) (string, error) {
	fmt.Println(`select s3db_version($1)`, table)
	r, err := db.Query(`select s3db_version($1)`, table)
	if err != nil {
		return "", fmt.Errorf("select: %w", err)
	}
	if !r.Next() {
		return "", nil
	}
	var v string
	err = r.Scan(&v)
	if err != nil {
		return "", fmt.Errorf("scan: %w", err)
	}
	r.Close()
	return v, nil
}
