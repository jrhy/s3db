package main

import (
	"fmt"

	"github.com/jrhy/s3db"
)

func init() {
	cmd := "diff"
	usage := `diff <fromVersion> [<toVersion>]`
	desc := `Shows differences between two versions.`
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		al := len(sa.Arg)
		if al == 0 || al > 2 {
			fmt.Fprintln(sa.Stderr, usage)
			return 1
		}
		fromVersion := sa.Arg[0]
		from := open(sa.Ctx, &s3db.OpenOptions{
			SingleVersion: fromVersion,
			ReadOnly:      true,
		}, sa)
		var toVersion string
		if al > 1 {
			toVersion = sa.Arg[1]
		}
		to := open(sa.Ctx, &s3db.OpenOptions{
			SingleVersion: toVersion,
			ReadOnly:      true,
		}, sa)
		err := to.Diff(sa.Ctx, from, sa.diff())
		if err != nil {
			fmt.Fprintln(sa.Stderr, err)
			return 1
		}
		sa.Result.suppressCommit = true
		return 0
	}
}

func (sa *subcommandArgs) diff() func(interface{}, interface{}, interface{}) (bool, error) {
	return func(key, myValue, fromValue interface{}) (keepGoing bool, err error) {
		if fromValue != nil {
			fmt.Fprintf(sa.Stdout, "-%v: %v\n", key, fromValue)
		}
		if myValue != nil {
			fmt.Fprintf(sa.Stdout, "+%v: %v\n", key, myValue)
		}
		return true, nil
	}
}
