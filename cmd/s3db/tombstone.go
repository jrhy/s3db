package main

import (
	"fmt"
	"os"
	"time"
)

func init() {
	cmd := "tombstone"
	usage := `tombstone <key> ...`
	desc := `Marks entry as deleted, un-settable.`
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		db := open(sa.Ctx, nil, sa)
		keys := []string{}
		if len(sa.Arg) == 0 {
			fmt.Fprintln(sa.Stderr, usage)
			return 1
		}
		for _, key := range sa.Arg {
			var err error
			var tombstoned bool
			if tombstoned, err = db.IsTombstoned(sa.Ctx, key); tombstoned {
				if !sa.Quiet {
					fmt.Fprintf(os.Stderr, "warning: '%s' already tombstoned\n", key)
				}
				continue
			}
			if err != nil {
				err = fmt.Errorf("tombstone '%s': %w", keys, err)
				fmt.Fprintln(sa.Stderr, err)
				return 1
			}
			var value string
			var ok bool
			if ok, err = db.Get(sa.Ctx, key, &value); !ok {
				if !sa.Quiet {
					fmt.Fprintf(os.Stderr, "warning: '%s' not previously set; adding tombstone anyway\n", key)
				}
			}
			if err != nil {
				err = fmt.Errorf("tombstone '%s': %w", keys, err)
				fmt.Fprintln(sa.Stderr, err)
				return 1
			}
			err = db.Tombstone(sa.Ctx, time.Now(), key)
			if err != nil {
				err = fmt.Errorf("tombstone '%s': %w", keys, err)
				fmt.Fprintln(sa.Stderr, err)
				return 1
			}
		}
		return 0
	}
}
