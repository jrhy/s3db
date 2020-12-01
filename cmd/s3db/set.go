package main

import (
	"fmt"
	"strings"
	"time"
)

func init() {
	cmd := "set"
	usage := `set <key>=<value> ...`
	desc := `Sets values for entries.`
	subcommandUsage[cmd] = usage
	subcommandDesc[cmd] = desc
	subcommandFuncs[cmd] = func(sa *subcommandArgs) int {
		if len(sa.Arg) == 0 {
			fmt.Fprintln(sa.Stderr, usage)
			return 1
		}
		db := open(sa.Ctx, nil, sa)
		keys := []string{}
		values := []interface{}{}
		for _, arg := range sa.Arg {
			a := strings.Split(arg, "=")
			if len(a) < 2 {
				fmt.Fprintln(sa.Stderr, usage)
				return 1
			}
			keys = append(keys, a[0])
			values = append(values, strings.Join(a[1:], "="))
		}
		for i := range keys {
			var cur interface{}
			var err error
			var tombstoned, ok bool
			if tombstoned, err = db.IsTombstoned(sa.Ctx, keys[i]); err == nil && tombstoned {
				if !sa.Quiet {
					fmt.Fprintf(sa.Stderr, "warning: set of '%s' is ineffective while tombstoned\n", keys[i])
				}
				continue
			} else if ok, err = db.Get(sa.Ctx, keys[i], &cur); ok {
				if !sa.Quiet && cur == values[i] {
					fmt.Fprintf(sa.Stderr, "warning: '%s' already had requested value; updating time only\n", keys[i])
				}
			}
			if err == nil {
				err = db.Set(sa.Ctx, time.Now(), keys[i], values[i])
			}
			if err != nil {
				err = fmt.Errorf("set '%s': %w", keys[i], err)
				fmt.Fprintln(sa.Stderr, err)
				return 1
			}
		}
		return 0
	}
}
