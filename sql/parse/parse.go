package parse

import (
	"encoding/json"
	"regexp"
	"strings"
)

type Parser struct {
	Remaining  string
	LastReject string
}

func (b *Parser) Copy() *Parser {
	ne := *b
	return &ne
}

func (b *Parser) SkipWS() bool {
	b.Remaining = strings.TrimLeft(b.Remaining, " \t\r\n")
	return true
}

func (b *Parser) Exact(prefix string) bool {
	if strings.HasPrefix(b.Remaining, prefix) {
		b.Remaining = b.Remaining[len(prefix):]
		return true
	}
	return false
}

func (b *Parser) CI(prefix string) bool {
	if strings.HasPrefix(strings.ToLower(b.Remaining),
		strings.ToLower(prefix)) {
		b.Remaining = b.Remaining[len(prefix):]
		return true
	}
	return false
}

func (b *Parser) String() string {
	bs, err := json.MarshalIndent(*b, "", " ")
	if err != nil {
		panic(err)
	}
	return string(bs)
}

type Func func(*Parser) bool

func SeqWS(fns ...Func) Func {
	return func(b *Parser) bool {
		e := b.Copy()
		for _, f := range fns {
			e.SkipWS()
			if !f(e) {
				if len(e.Remaining) < len(b.LastReject) {
					b.LastReject = e.Remaining
				}
				return false
			}
			e.SkipWS()
		}
		*b = *e
		return true
	}
}

func CI(s string) Func {
	return func(b *Parser) bool {
		return b.CI(s)
	}
}

func Exact(s string) Func {
	return func(b *Parser) bool {
		return b.Exact(s)
	}
}

func (b *Parser) Match(f func(*Parser) bool) bool {
	return f(b)
}

func (m Func) Action(then func()) Func {
	return func(b *Parser) bool {
		if m(b) {
			then()
			return true
		}
		return false
	}
}

func (m Func) Or(other Func) Func {
	return func(b *Parser) bool {
		if m(b) {
			return true
		}
		return other(b)
	}
}

func OneOf(fns ...func(*Parser) bool) Func {
	return func(b *Parser) bool {
		for _, f := range fns {
			if f(b) {
				return true
			}
		}
		return false
	}
}

func Optional(f Func) Func {
	return func(b *Parser) bool {
		e := b.Copy()
		if e.Match(f) {
			*b = *e
		}
		return true
	}
}

func AtLeastOne(f Func) Func {
	return func(b *Parser) bool {
		e := b.Copy()
		if !e.Match(f) {
			return false
		}
		for e.Match(f) {
		}
		*b = *e
		return true
	}
}

func Multiple(f Func) Func {
	return func(b *Parser) bool {
		e := b.Copy()
		for e.Match(f) {
		}
		*b = *e
		return true
	}
}

func RE(re *regexp.Regexp, submatchcb func([]string) bool) Func {
	if !strings.HasPrefix(re.String(), "^") {
		panic("regexp missing ^ restriction: " + re.String())
	}
	return func(b *Parser) bool {
		s := re.FindStringSubmatch(b.Remaining)
		if s != nil && submatchcb != nil && submatchcb(s) {
			if !strings.HasPrefix(b.Remaining, s[0]) {
				panic("pattern must restrict all alternatives to match at beginning: " + re.String())
			}
			b.Remaining = b.Remaining[len(s[0]):]
			return true
		}
		return false
	}
}

func Delimited(
	term Func, delimiter Func) Func {
	return func(e *Parser) bool {
		terms := 0
		for {
			if !term(e) {
				break
			}
			terms++
			if !delimiter(e) {
				break
			}
		}
		return terms > 0
	}
}

func End() Func {
	return func(e *Parser) bool {
		return e.Remaining == ""
	}
}
