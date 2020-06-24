package util

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
)

var (
	alphabet []byte
)

func init() {
	alphabet = make([]byte, 26)
	for i := 0; i < 26; i++ {
		alphabet[i] = 'a' + byte(i)
	}
}

func TestFastRegexMatcher(t *testing.T) {
	_, err := NewFastRegexMatcher("kafka_producer_producer.*_pending")
	require.NoError(t, err)
}

func BenchmarkFastMatcher_Regex(b *testing.B) {
	tests := map[string]struct {
		values []string
		regexp string
	}{
		"regexp with literal prefix: high cardinality, low matches": {
			values: append(generateLabelValues(100000), generateLabelValuesWithPrefix("kafka_producer_producer", 2500)...),
			regexp: "kafka_producer_producer.*",
		},
		"regexp with literal prefix: high cardinality, high matches": {
			values: generateLabelValuesWithPrefix("kafka_producer_producer", 100000),
			regexp: "kafka_producer_producer.*",
		},
		"regexp with literal prefix: low cardinality, 100% matches": {
			values: generateLabelValuesWithPrefix("kafka_producer_producer", 100),
			regexp: "kafka_producer_producer.*",
		},
		"regexp with literal suffix: high cardinality, low matches": {
			values: append(generateLabelValues(100000), generateLabelValuesWithSuffix("_pending", 2500)...),
			regexp: ".*_pending",
		},
		"regexp with literal suffix: high cardinality, 100% matches": {
			values: generateLabelValuesWithSuffix("_pending", 100000),
			regexp: ".*_pending",
		},
		"regexp with literal suffix: low cardinality, 100% matches": {
			values: generateLabelValuesWithSuffix("_pending", 100),
			regexp: ".*_pending",
		},
		"regexp with literal prefix and suffix: high cardinality, low matches": {
			values: append(generateLabelValues(100000), generateLabelValuesWithPrefixAndSuffix("kafka_producer_producer", "_pending", 2500)...),
			regexp: "kafka_producer_producer.*_pending",
		},
		"regexp with literal prefix and suffix: high cardinality, 100% matches": {
			values: generateLabelValuesWithPrefixAndSuffix("kafka_producer_producer", "_pending", 100000),
			regexp: "kafka_producer_producer.*_pending",
		},
		"regexp with literal prefix and suffix: low cardinality, 100% matches": {
			values: generateLabelValuesWithPrefixAndSuffix("kafka_producer_producer", "_pending", 100),
			regexp: "kafka_producer_producer.*_pending",
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			m := MustNewFastMatcher(labels.MatchRegexp, "name", testData.regexp)
			for n := 0; n < b.N; n++ {
				for _, value := range testData.values {
					m.Matches(value)
				}
			}
		})
	}
}

func generateLabelValues(num int) []string {
	res := make([]string, num)

	for i := 1; i <= num; i++ {
		value := ""
		for c := i; c > 0; c = c / len(alphabet) {
			value = value + string(alphabet[c%len(alphabet)])
		}
		res[i-1] = value
	}

	return res
}

func generateLabelValuesWithPrefix(prefix string, num int) []string {
	res := make([]string, num)
	for i, val := range generateLabelValues(num) {
		res[i] = prefix + val
	}
	return res
}

func generateLabelValuesWithSuffix(suffix string, num int) []string {
	res := make([]string, num)
	for i, val := range generateLabelValues(num) {
		res[i] = val + suffix
	}
	return res
}

func generateLabelValuesWithPrefixAndSuffix(prefix, suffix string, num int) []string {
	res := make([]string, num)
	for i, val := range generateLabelValues(num) {
		res[i] = prefix + val + suffix
	}
	return res
}
