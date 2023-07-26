package main

import (
	"database/sql"
	"errors"
	"fmt"

	// register s3db extension with riyazali.net/sqlite
	_ "github.com/jrhy/s3db/sqlite/mod"
	// autoload riyazali.net/sqlite-registered extensions in sqlite
	_ "github.com/jrhy/s3db/sqlite/sqlite-autoload-extension"
	// mattn's awesome sql driver for sqlite
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	var db *sql.DB
	var err error
	if db, err = sql.Open("sqlite3", ""); err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return err
	}
	_, err = db.Exec("create virtual table f using s3db (columns='a primary key, b')")
	if err != nil {
		return err
	}
	_, err = db.Exec("insert into f values(1,1)")
	if err != nil {
		return err
	}
	r, err := db.Query("select * from f")
	if err != nil {
		return err
	}
	if !r.Next() {
		return errors.New("something went wrong! where did the first row go?")
	}
	if cols, err := r.Columns(); err != nil {
		return err
	} else {
		fmt.Printf("columns: %+v\n", cols)
	}
	var a, b int64
	err = r.Scan(&a, &b)
	if err != nil {
		return err
	}
	if a != int64(1) || b != int64(1) {
		return fmt.Errorf("something went wrong! the first row should be (1,1) but is (%d,%d)!", a, b)
	}
	fmt.Printf("the first row is: [%d %d]\n", a, b)
	return nil
}
