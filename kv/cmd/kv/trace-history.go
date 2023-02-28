package main

import (
	"fmt"
	"time"

	"github.com/jrhy/s3db/kv"
)

func init() {
	cmd := "trace-history"
	usage := "trace-history [--newer-than=<N(s|m|h)>] <key> ..."
	desc := "Shows historic values of keys."
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		var d time.Duration
		afterTime := time.Time{}
		if sa.SubcommandOptions["--newer-than"] != nil {
			err := parseDuration(&sa.SubcommandOptions, "--newer-than", &d)
			if err != nil {
				fmt.Fprintf(sa.Stderr, "trace-history: %v\n", err)
				return 1
			}
			afterTime = time.Now().Add(-d)
		}
		db := open(sa.Ctx, &kv.OpenOptions{ReadOnly: true}, sa)
		keys := []string{}
		if len(sa.Arg) == 0 {
			fmt.Fprintln(sa.Stderr, usage)
			return 1
		}
		for _, key := range sa.Arg {
			var err error
			err = db.TraceHistory(sa.Ctx, key, afterTime, func(when time.Time, value interface{}) (keepGoing bool, err error) {
				if value != nil {
					fmt.Fprintf(sa.Stdout, "%v %s: %s\n", when, key, value)
				} else {
					fmt.Fprintf(sa.Stdout, "%v %s tombstoned\n", when, key)
				}
				return true, nil
			})
			if err != nil {
				err = fmt.Errorf("traceHistory '%s': %w", keys, err)
				fmt.Fprintln(sa.Stderr, err)
				return 1
			}
		}
		return 0
	}
}
