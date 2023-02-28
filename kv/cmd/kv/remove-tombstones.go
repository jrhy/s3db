package main

import (
	"fmt"
	"time"
)

func init() {
	cmd := "remove-tombstones"
	usage := "remove-tombstones --older-than=<N(s|m|h)>"
	desc := "Removes tombstone entries older than a certain age."
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		var d time.Duration
		err := parseDuration(&sa.SubcommandOptions, "--older-than", &d)
		if err != nil {
			err = fmt.Errorf("remove-tombstones: %w", err)
			fmt.Fprintln(sa.Stderr, err)
			return 1
		}
		db := open(sa.Ctx, nil, sa)
		err = db.RemoveTombstones(sa.Ctx, time.Now().Add(-d))
		if err != nil {
			err = fmt.Errorf("remove-tombstones: %w", err)
			fmt.Fprintln(sa.Stderr, err)
			return 1
		}
		return 0
	}
}
