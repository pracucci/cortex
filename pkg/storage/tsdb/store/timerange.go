package store

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// TimeRange holds a time range expressed in milliseconds. Both mint
// and maxt are inclusive.
type TimeRange struct {
	mint int64
	maxt int64
}

// MinTime return mint as time.Time.
func (t *TimeRange) MinTime() time.Time {
	return time.Unix(t.mint/1000, t.mint%1000).UTC()
}

// MaxTime return maxt as time.Time.
func (t *TimeRange) MaxTime() time.Time {
	return time.Unix(t.maxt/1000, t.maxt%1000).UTC()
}

// String returns an human readable representation of the time range.
func (t *TimeRange) String() string {
	return fmt.Sprintf("[%s,%s]", t.MinTime().Format(time.RFC3339Nano), t.MaxTime().Format(time.RFC3339Nano))
}

// TimeRanges is a list of TimeRange sorted by mint.
type TimeRanges []TimeRange

// Add a TimeRange to the list.
func (r *TimeRanges) Add(t TimeRange) {
	pos := r.insert(t)

	// Once has been inserted, we have to merge adjacent or overlapping
	// time ranges.
	startMerge := pos
	endMerge := pos

	for ; startMerge > 0 && (*r)[pos].mint <= (*r)[startMerge-1].maxt+1; startMerge-- {
	}

	for ; endMerge < len(*r)-1 && (*r)[pos].maxt >= (*r)[endMerge+1].mint-1; endMerge++ {
	}

	if startMerge < endMerge {
		r.merge(startMerge, endMerge)
	}
}

// Xor returns a list of time ranges which are missing (mint and maxt included)
// in the current ranges, compared to the input time range.
func (r *TimeRanges) Xor(t TimeRange) TimeRanges {
	if len(*r) == 0 {
		return TimeRanges{t}
	}

	// Look for the position of the first range starting after the input one,
	// because below we do iterate starting from it and we add missing segments
	// on the LEFT.
	pos := sort.Search(len(*r), func(pos int) bool {
		return (*r)[pos].mint > t.mint
	})

	missing := TimeRanges{}

	// We iterate over the ranges and we check if we have to add a segment
	// to the LEFT of pos.
	for ; pos < len(*r) && (pos == 0 || (*r)[pos-1].maxt < t.maxt); pos++ {
		// Given we always add the missing segment on the left, the mint
		// depends on the maxt of the previous range (if any).
		var mint int64

		if pos > 0 {
			mint = max(t.mint, (*r)[pos-1].maxt+1)
		} else {
			mint = t.mint
		}

		missing = append(missing, TimeRange{
			mint: mint,
			maxt: min(t.maxt, (*r)[pos].mint-1),
		})

		// We're covered until the end of the current range, so we can
		// increase it.
		t.mint = (*r)[pos].maxt + 1
	}

	// If the iteration reached the end of the ranges, we have to check if there's
	// a missing segment after the last range.
	if pos > 0 && pos == len(*r) && t.maxt > (*r)[pos-1].maxt {
		missing = append(missing, TimeRange{
			mint: max(t.mint, (*r)[pos-1].maxt+1),
			maxt: t.maxt,
		})
	}

	return missing
}

// Normalize returns a list of time ranges normalized by the input period. For
// example, if the input period is a day, then the output are the minimum number
// of ranges where each time range boundary is respectively rounded to the
// begin and end of the day.
func (r *TimeRanges) Normalize(period int64) TimeRanges {
	result := *r

	for pos := 0; pos < len(result); {
		t := result[pos]

		normalizedMint := (t.mint / period) * period
		normalizedMaxt := ((t.maxt / period) * period) + period - 1

		// If it's already normalized we can move on to the next one
		if t.mint == normalizedMint && t.maxt == normalizedMaxt {
			pos++
			continue
		}

		// To normalize and merge we add the normalized range and let
		// the Add() do the merge (if any)
		result.Add(TimeRange{normalizedMint, normalizedMaxt})
	}

	return result
}

// String returns a human readable representation of the time ranges.
func (r *TimeRanges) String() string {
	b := strings.Builder{}

	for _, t := range *r {
		b.WriteString(t.String())
	}

	return b.String()
}

func (r *TimeRanges) insert(t TimeRange) int {
	// Look for the position where the input TimeRange should be inserted,
	// honoring the guarantee that the list is sorted by mint.
	i := sort.Search(len(*r), func(pos int) bool {
		return t.mint <= (*r)[pos].mint
	})

	// Insert the item in the right position.
	*r = append(*r, TimeRange{})
	copy((*r)[i+1:], (*r)[i:])
	(*r)[i] = t

	return i
}

func (r *TimeRanges) merge(start, end int) {
	// Find the maximum timestamp within the range to merge
	maxt := (*r)[start].maxt
	for i := start + 1; i <= end; i++ {
		if (*r)[i].maxt > maxt {
			maxt = (*r)[i].maxt
		}
	}

	// Overwrite the first item in the range to merge, with the merged time range.
	// Since it's ordered by mint, the minimum is the first item, while the maximum
	// should be updated.
	(*r)[start].maxt = maxt

	// Remove the items between start+1 and end (included).
	copy((*r)[start+1:], (*r)[end+1:])
	*r = (*r)[:len(*r)-(end-start)]
}

func min(first, second int64) int64 {
	if first < second {
		return first
	}

	return second
}

func max(first, second int64) int64 {
	if first > second {
		return first
	}

	return second
}
