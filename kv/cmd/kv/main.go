package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docopt/docopt-go"
	"github.com/jrhy/s3db/kv"
)

const version = "0.1"

var (
	subcommandFuncs = map[string]func(*subcommandArgs) int{}
	subcommandUsage = map[string]string{}
	subcommandDesc  = map[string]string{}
)

type subcommandArgs struct {
	// inputs
	Bucket        string
	Prefix        string   `docopt:"-p,--prefix"`
	MasterKeyFile string   `docopt:"-k,--master-key-file"`
	Quiet         bool     `docopt:"-q,--quiet"`
	Verbose       bool     `docopt:"-v,--verbose"`
	Subcommand    string   `docopt:"<command>"`
	Arg           []string `docopt:"<arg>"`
	Ctx           context.Context
	Stdout        io.Writer
	Stderr        io.Writer

	// derived
	encryptor         kv.Encryptor
	SubcommandOptions docopt.Opts

	// outputs
	db     *kv.DB
	s3opts *kv.OpenOptions
	Result struct {
		suppressCommit bool
	}
}

func main() {
	s := subcommandArgs{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Ctx:    context.Background(),
	}
	os.Exit(int(s.run(os.Args[1:])))
}

func parseArgs(s *subcommandArgs, args []string) {
	usage := `kv v` + version + `

Usage:
  kv --bucket=<name> [--master-key-file=<path>] [--prefix=<s3-prefix>] 
	   [-qv] <command> [<arg>...]
  kv -h

Options:
  -b, --bucket=<name>    S3 bucket to put the database in
  -h, --help             Print detailed help, including subcommands.
  -k, --master-key-file=<path>
                         path to master key material bytes
  -p, --prefix=<string>  S3 object name prefix
  -q, --quiet            suppress warnings
  -v, --verbose          always say what happened

Environment:
  S3_ENDPOINT=<url>      override S3 endpoint, if not using AWS S3
                         (e.g. minio, Wasabi)
  AWS SDK                S3 client per
                         docs.aws.amazon.com/cli/latest/reference/configure

Commands:
`
	cmds := []string{}
	for cmd := range subcommandUsage {
		cmds = append(cmds, cmd)
	}
	sort.Strings(cmds)
	for _, cmd := range cmds {
		usage += fmt.Sprintf("  %s\n", subcommandUsage[cmd])
		usage += fmt.Sprintf("    %s\n", subcommandDesc[cmd])
	}
	p := docopt.Parser{
		OptionsFirst: true,
	}
	opts, err := p.ParseArgs(usage, args, version)
	if err != nil {
		panic(err)
	}
	err = opts.Bind(s)
	if err != nil {
		panic(err)
	}
}

func (s *subcommandArgs) run(args []string) int {
	parseArgs(s, args)
	if s.MasterKeyFile != "" {
		keyBytes, err := ioutil.ReadFile(s.MasterKeyFile)
		if err != nil {
			fmt.Fprintln(s.Stderr, err)
			return 1
		}
		s.encryptor = kv.V1NodeEncryptor(keyBytes)
	}
	if f, ok := subcommandFuncs[s.Subcommand]; ok {
		su := subcommandUsage[s.Subcommand]
		var r int
		r = parseSubcommandArgs(su, s)
		if r != 0 {
			return r
		}
		r = f(s)
		if r != 0 {
			return r
		}
	} else {
		fmt.Fprintf(s.Stderr, "unknown command: %s", s.Subcommand)
		fmt.Fprintf(s.Stderr, "arg: %v\n", s.Arg)
		return 1
	}
	if s.db == nil ||
		s.s3opts == nil ||
		s.s3opts.ReadOnly ||
		s.Result.suppressCommit {
		return 0
	}
	if !s.db.IsDirty() {
		if s.Verbose {
			fmt.Fprintf(s.Stdout, "no change\n")
		}
		return 0
	}
	hash, err := s.db.Commit(s.Ctx)
	if err != nil {
		fmt.Fprintln(s.Stderr, err)
		return 1
	}
	if s.Verbose {
		if hash != nil {
			fmt.Fprintf(s.Stdout, "committed %s\n", *hash)
		} else {
			fmt.Fprintf(s.Stdout, "committed empty tree\n")
		}
	}
	return 0
}

func open(ctx context.Context, opts *kv.OpenOptions, args *subcommandArgs) *kv.DB {
	if args.Bucket == "" {
		fmt.Fprintf(args.Stderr, "--bucket not set\n")
		os.Exit(1)
	}
	s := getS3()
	if s.Endpoint == "" {
		fmt.Fprintf(args.Stderr, "No S3 endpoint configured. Ensure AWS SDK is configured or set S3_ENDPOINT explicitly.\n")
		os.Exit(1)
	}

	cfg := kv.Config{
		Storage: &kv.S3BucketInfo{
			EndpointURL: s.Endpoint,
			BucketName:  args.Bucket,
			Prefix:      args.Prefix,
		},
		KeysLike:      "stringy",
		ValuesLike:    "stringy",
		NodeEncryptor: args.encryptor,
	}
	var so kv.OpenOptions
	if opts != nil {
		so = *opts
	}
	db, err := kv.Open(ctx, s, cfg, so, time.Now())
	if err != nil {
		err = fmt.Errorf("open: %w", err)
		fmt.Fprintln(args.Stderr, err)
		os.Exit(1)
	}

	args.db = db
	args.s3opts = &so
	return db
}

func getS3() *s3.S3 {
	config := aws.Config{}
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint != "" {
		config.Endpoint = &endpoint
		config.S3ForcePathStyle = aws.Bool(true)
	}

	sess, err := session.NewSession(&config)
	if err != nil {
		err = fmt.Errorf("session: %w", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return s3.New(sess)
}

func parseSubcommandArgs(usage string, s *subcommandArgs) int {
	p := docopt.Parser{
		SkipHelpFlags: true,
	}
	opts, err := p.ParseArgs(
		"Usage: "+strings.Split(usage, "\n")[0],
		s.Arg, "")
	if err != nil {
		fmt.Fprintln(s.Stderr, err)
		return 1
	}
	s.SubcommandOptions = opts
	return 0
}

func parseDuration(o *docopt.Opts, name string, d *time.Duration) error {
	durstr, err := o.String(name)
	if err != nil {
		return fmt.Errorf("option: %w", err)
	}
	if durstr == "" {
		return errors.New("empty duration")
	}
	*d, err = time.ParseDuration(durstr)
	if err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	return nil
}
