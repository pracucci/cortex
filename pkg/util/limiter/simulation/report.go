package main

import (
	"fmt"
	"strings"
)

type report struct {
	// Simulation config
	name               string
	limit              int
	bucketPeriod       int
	distributors       int
	clientTokensPerReq int
	clientReqsPerSec   int

	// State
	buckets map[int64]*bucketReport
}

func newReport(name string, limit, bucketPeriod, distributors, clientTokensPerReq, clientReqsPerSec int) *report {
	return &report{
		name:               name,
		limit:              limit,
		bucketPeriod:       bucketPeriod,
		distributors:       distributors,
		clientTokensPerReq: clientTokensPerReq,
		clientReqsPerSec:   clientReqsPerSec,
		buckets:            map[int64]*bucketReport{},
	}
}

func (r *report) addAccepted(bucketKey int64, tokens int) {
	r.getBucketReport(bucketKey).addAccepted(tokens)
}

func (r *report) addBlocked(bucketKey int64, tokens int) {
	r.getBucketReport(bucketKey).addBlocked(tokens)
}

func (r *report) getBucketReport(bucketKey int64) *bucketReport {
	if bucket, ok := r.buckets[bucketKey]; ok {
		return bucket
	}

	bucket := &bucketReport{
		bucket: bucketKey,
	}
	r.buckets[bucketKey] = bucket

	return bucket
}

func (r *report) String() string {
	builder := strings.Builder{}

	// Simulation setup
	builder.WriteString(fmt.Sprintf("Limit:             %d tokens / sec\n", r.limit))
	builder.WriteString(fmt.Sprintf("Bucket period:     %d sec\n", r.bucketPeriod))
	builder.WriteString(fmt.Sprintf("Distributors:      %d\n", r.distributors))
	builder.WriteString(fmt.Sprintf("Client req/s:      %d\n", r.clientReqsPerSec))
	builder.WriteString(fmt.Sprintf("Client tokens/req: %d\n", r.clientTokensPerReq))
	builder.WriteString("\n")

	// Details of each bucket
	for _, bucket := range r.buckets {
		builder.WriteString(bucket.String())
		builder.WriteString("\n")
	}
	builder.WriteString("\n")

	// Average tokens / sec
	builder.WriteString(fmt.Sprintf("Avg accepted tokens / sec: %.2f", r.avgAcceptedTokensPerSec()))

	return builder.String()
}

func (r *report) CSVHeader() string {
	return "name,limit,bucket period,distributors,client req/s,client tokens/req,avg accepted tokens/s"
}

func (r *report) CSVEntry() string {
	return fmt.Sprintf("%s,%d,%d,%d,%d,%d,%.0f", r.name, r.limit, r.bucketPeriod, r.distributors, r.clientReqsPerSec, r.clientTokensPerReq, r.avgAcceptedTokensPerSec())
}

func (r *report) avgAcceptedTokensPerSec() float64 {
	acceptedCount := 0

	for _, bucket := range r.buckets {
		acceptedCount += bucket.tokensAccepted
	}

	return float64(acceptedCount) / float64(len(r.buckets)*r.bucketPeriod)
}

type bucketReport struct {
	bucket         int64
	tokensAccepted int
	tokensBlocked  int
}

func (r *bucketReport) addAccepted(tokens int) {
	r.tokensAccepted += tokens
}

func (r *bucketReport) addBlocked(tokens int) {
	r.tokensBlocked += tokens
}

func (r *bucketReport) String() string {
	return fmt.Sprintf("Bucket %d Accepted: %5d Blocked: %5d", r.bucket, r.tokensAccepted, r.tokensBlocked)
}
