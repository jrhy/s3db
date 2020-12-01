package main

import (
	"fmt"
	"os"
)

func init() {
	cmd := "merge"
	usage := "merge"
	desc := "Prints version name resulting from merging current versions."
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		db := open(sa.Ctx, nil, sa)
		name, err := db.Commit(sa.Ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if name != nil {
			fmt.Fprintf(sa.Stdout, "%s\n", *name)
		} else {
			fmt.Fprintf(sa.Stdout, "\n")
		}
		sa.Result.suppressCommit = true
		return 0
	}
}
