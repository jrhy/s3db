package auto

// #cgo CFLAGS: -DSQLITE_CORE
//
// #include "sqlite3.h"
// extern int sqlite3_extension_init(sqlite3*, char**, const sqlite3_api_routines*);
import "C"

// invokes sqlite3_extension_init() in go.riyazali.net/sqlite that
// makes available to all future database connections, all the
// Go sqlite extensions that have been registered.
func init() { C.sqlite3_auto_extension((*[0]byte)(C.sqlite3_extension_init)) }
