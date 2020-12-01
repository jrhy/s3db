package main

import (
	"fmt"

	"github.com/jrhy/s3db"
)

func init() {
	cmd := "show"
	usage := cmd
	desc := `Prints entries to stdout.`
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		db := open(sa.Ctx, &s3db.OpenOptions{ReadOnly: true}, sa)
		err := db.Diff(sa.Ctx, nil, sa.dump())
		if err != nil {
			fmt.Fprintln(sa.Stderr, err)
			return 1
		}
		return 0
	}
}

func (sa *subcommandArgs) dump() func(interface{}, interface{}, interface{}) (bool, error) {
	return func(key, myValue, fromValue interface{}) (keepGoing bool, err error) {
		fmt.Fprintf(sa.Stdout, "%v: %v\n", key, myValue)
		return true, nil
	}
}
