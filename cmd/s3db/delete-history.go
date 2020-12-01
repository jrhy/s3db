package main

import (
	"fmt"
	"time"
)

func init() {
	cmd := "delete-history"
	subcommandUsage[cmd] = "delete-history --older-than=<N(s|m|h)>"
	subcommandDesc[cmd] = "Removes versions and associated nodes older than a certain age."
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		var d time.Duration
		err := parseDuration(&sa.SubcommandOptions, "--older-than", &d)
		if err != nil {
			err = fmt.Errorf("delete-history: %w", err)
			fmt.Fprintln(sa.Stderr, err)
			return 1
		}
		db := open(sa.Ctx, nil, sa)
		err = db.DeleteHistoryBefore(sa.Ctx, time.Now().Add(-d))
		if err != nil {
			err = fmt.Errorf("delete-history: %w", err)
			fmt.Fprintln(sa.Stderr, err)
			return 1
		}
		sa.Result.suppressCommit = true
		return 0
	}
}
