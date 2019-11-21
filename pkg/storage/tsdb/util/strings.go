package util

// CommonPrefix returns the longest common prefix between the two input strings.
func CommonPrefix(first, second string) string {
	i := 0

	// Keep iterating until the prefix is the same
	for ; i < len(first) && i < len(second) && first[i] == second[i]; i++ {
	}

	return first[:i]
}
