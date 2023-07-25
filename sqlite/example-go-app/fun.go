package main

import (
	"database/sql"

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
	return nil
}
