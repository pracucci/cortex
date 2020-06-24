package util

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
)

// FastMatcher models the matching of a label.
type FastMatcher struct {
	Type  labels.MatchType
	Name  string
	Value string

	re *FastRegexMatcher
}

// NewFastMatcher returns a matcher object.
func NewFastMatcher(t labels.MatchType, n, v string) (*FastMatcher, error) {
	m := &FastMatcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == labels.MatchRegexp || t == labels.MatchNotRegexp {
		re, err := NewFastRegexMatcher(v)
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// MustNewFastMatcher panics on error - only for use in tests!
func MustNewFastMatcher(mt labels.MatchType, name, val string) *FastMatcher {
	m, err := NewFastMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}

func (m *FastMatcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// Matches returns whether the matcher matches the given string value.
func (m *FastMatcher) Matches(s string) bool {
	switch m.Type {
	case labels.MatchEqual:
		return s == m.Value
	case labels.MatchNotEqual:
		return s != m.Value
	case labels.MatchRegexp:
		return m.re.MatchString(s)
	case labels.MatchNotRegexp:
		return !m.re.MatchString(s)
	}
	panic("labels.FastMatcher.Matches: invalid match type")
}

// Inverse returns a matcher that matches the opposite.
func (m *FastMatcher) Inverse() (*FastMatcher, error) {
	switch m.Type {
	case labels.MatchEqual:
		return NewFastMatcher(labels.MatchNotEqual, m.Name, m.Value)
	case labels.MatchNotEqual:
		return NewFastMatcher(labels.MatchEqual, m.Name, m.Value)
	case labels.MatchRegexp:
		return NewFastMatcher(labels.MatchNotRegexp, m.Name, m.Value)
	case labels.MatchNotRegexp:
		return NewFastMatcher(labels.MatchRegexp, m.Name, m.Value)
	}
	panic("labels.FastMatcher.Matches: invalid match type")
}

// GetRegexString returns the regex string.
func (m *FastMatcher) GetRegexString() string {
	if m.re == nil {
		return ""
	}
	return m.re.String()
}

type FastRegexMatcher struct {
	re     *regexp.Regexp
	prefix string
	suffix string
}

func NewFastRegexMatcher(v string) (*FastRegexMatcher, error) {
	re, err := regexp.Compile("^(?:" + v + ")$")
	if err != nil {
		return nil, err
	}
	m := &FastRegexMatcher{
		re: re,
	}

	parsed, err := syntax.Parse(v, syntax.Perl)
	if err != nil {
		return nil, err
	}

	// SUPER HACK
	if parsed.Op == syntax.OpConcat {
		if parsed.Sub[0].Op == syntax.OpLiteral {
			m.prefix = string(parsed.Sub[0].Rune)
		}

		if parsed.Sub[len(parsed.Sub)-1].Op == syntax.OpLiteral {
			m.suffix = string(parsed.Sub[len(parsed.Sub)-1].Rune)
		}
	}

	return m, nil
}

func (m *FastRegexMatcher) MatchString(s string) bool {
	if m.prefix != "" && !strings.HasPrefix(s, m.prefix) {
		return false
	}
	if m.suffix != "" && !strings.HasSuffix(s, m.suffix) {
		return false
	}
	return m.re.MatchString(s)
}

// GetRegexString returns the regex string.
func (m *FastRegexMatcher) String() string {
	return m.re.String()
}
