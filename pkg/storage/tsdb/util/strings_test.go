package util

import "testing"

import "github.com/stretchr/testify/assert"

func Test_CommonPrefix(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		first    string
		second   string
		expected string
	}{
		"both input strings are empty": {
			first:    "",
			second:   "",
			expected: "",
		},
		"no common prefix": {
			first:    "foo",
			second:   "bar",
			expected: "",
		},
		"both strings are equal": {
			first:    "foo",
			second:   "foo",
			expected: "foo",
		},
		"first string is fully within the second one": {
			first:    "foo",
			second:   "foot",
			expected: "foo",
		},
		"second string is fully within the first one": {
			first:    "foot",
			second:   "foo",
			expected: "foo",
		},
		"both strings have some prefix in common": {
			first:    "aa123",
			second:   "aa456",
			expected: "aa",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, CommonPrefix(testData.first, testData.second))
		})
	}
}
