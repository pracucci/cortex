package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimeRanges_Add(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		actual   TimeRanges
		add      TimeRange
		expected TimeRanges
	}{
		"initial empty ranges": {
			actual:   TimeRanges{},
			add:      TimeRange{5, 6},
			expected: TimeRanges{{5, 6}},
		},
		"append non overlapping time range at the beginning": {
			actual:   TimeRanges{{5, 6}},
			add:      TimeRange{2, 3},
			expected: TimeRanges{{2, 3}, {5, 6}},
		},
		"append non overlapping time range at the end": {
			actual:   TimeRanges{{5, 6}},
			add:      TimeRange{8, 9},
			expected: TimeRanges{{5, 6}, {8, 9}},
		},
		"append non overlapping time range in the middle": {
			actual:   TimeRanges{{1, 2}, {8, 9}},
			add:      TimeRange{4, 5},
			expected: TimeRanges{{1, 2}, {4, 5}, {8, 9}},
		},
		"merge the entire range with an input range larger than boundaries": {
			actual:   TimeRanges{{2, 3}, {5, 6}, {8, 9}},
			add:      TimeRange{1, 10},
			expected: TimeRanges{{1, 10}},
		},
		"merge the entire range with an input range smaller than boundaries": {
			actual:   TimeRanges{{2, 3}, {5, 6}, {8, 9}},
			add:      TimeRange{3, 7},
			expected: TimeRanges{{2, 9}},
		},
		"merge at the beginning": {
			actual:   TimeRanges{{2, 3}, {5, 6}, {8, 9}},
			add:      TimeRange{1, 4},
			expected: TimeRanges{{1, 6}, {8, 9}},
		},
		"merge in the middle": {
			actual:   TimeRanges{{1, 2}, {5, 6}, {8, 9}},
			add:      TimeRange{4, 5},
			expected: TimeRanges{{1, 2}, {4, 6}, {8, 9}},
		},
		"merge at the end": {
			actual:   TimeRanges{{1, 2}, {5, 6}, {8, 9}},
			add:      TimeRange{7, 7},
			expected: TimeRanges{{1, 2}, {5, 9}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			testData.actual.Add(testData.add)
			assert.Equal(t, testData.expected, testData.actual)
		})
	}
}

func TestTimeRanges_Xor(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		current  TimeRanges
		input    TimeRange
		expected TimeRanges
	}{
		"empty ranges": {
			current:  TimeRanges{},
			input:    TimeRange{2, 9},
			expected: TimeRanges{{2, 9}},
		},
		"missing range at the beginning (no overlaps)": {
			current:  TimeRanges{{6, 7}, {9, 10}},
			input:    TimeRange{1, 4},
			expected: TimeRanges{{1, 4}},
		},
		"missing in the middle (no overlaps)": {
			current:  TimeRanges{{1, 3}, {9, 10}},
			input:    TimeRange{4, 7},
			expected: TimeRanges{{4, 7}},
		},
		"missing at the end (no overlaps)": {
			current:  TimeRanges{{1, 3}, {5, 6}},
			input:    TimeRange{8, 10},
			expected: TimeRanges{{8, 10}},
		},
		"missing range at the beginning (with overlaps)": {
			current:  TimeRanges{{5, 7}, {9, 10}},
			input:    TimeRange{2, 6},
			expected: TimeRanges{{2, 4}},
		},
		"missing range in the middle (with overlaps)": {
			current:  TimeRanges{{5, 6}, {9, 10}},
			input:    TimeRange{5, 10},
			expected: TimeRanges{{7, 8}},
		},
		"missing range in the middle (with boundaries overlap)": {
			current:  TimeRanges{{5, 6}, {9, 10}},
			input:    TimeRange{6, 9},
			expected: TimeRanges{{7, 8}},
		},
		"missing range at the end (with overlaps)": {
			current:  TimeRanges{{5, 6}, {8, 10}},
			input:    TimeRange{9, 11},
			expected: TimeRanges{{11, 11}},
		},
		"input range is fully within the first range": {
			current:  TimeRanges{{1, 4}, {6, 10}},
			input:    TimeRange{2, 3},
			expected: TimeRanges{},
		},
		"input range is fully within the last range": {
			current:  TimeRanges{{1, 4}, {6, 10}},
			input:    TimeRange{7, 8},
			expected: TimeRanges{},
		},
		"input range matches the first range": {
			current:  TimeRanges{{1, 4}, {6, 10}},
			input:    TimeRange{1, 4},
			expected: TimeRanges{},
		},
		"input range matches the last range": {
			current:  TimeRanges{{1, 4}, {6, 10}},
			input:    TimeRange{6, 10},
			expected: TimeRanges{},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.current.Xor(testData.input)
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestTimeRanges_Normalize(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		input    TimeRanges
		period   int64
		expected TimeRanges
	}{
		"empty input": {
			input:    TimeRanges{},
			period:   5,
			expected: TimeRanges{},
		},
		"single range in input": {
			input:    TimeRanges{{8, 9}},
			period:   5,
			expected: TimeRanges{{5, 9}},
		},
		"multiple disjunct ranges after normalization": {
			input:    TimeRanges{{8, 9}, {16, 16}, {25, 31}},
			period:   5,
			expected: TimeRanges{{5, 9}, {15, 19}, {25, 34}},
		},
		"multiple conjunct ranges after normalization": {
			input:    TimeRanges{{8, 9}, {11, 16}, {21, 31}},
			period:   5,
			expected: TimeRanges{{5, 34}},
		},
		"multiple ranges, some of which are conjunct, after normalization": {
			input:    TimeRanges{{8, 9}, {11, 16}, {21, 31}, {44, 44}, {56, 57}, {58, 59}, {60, 61}},
			period:   5,
			expected: TimeRanges{{5, 34}, {40, 44}, {55, 64}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.input.Normalize(testData.period)
			assert.Equal(t, testData.expected, actual)
		})
	}
}
