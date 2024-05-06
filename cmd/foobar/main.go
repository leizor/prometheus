package main

import (
	"bufio"
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	commonconfig "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	rawURL                 = "http://localhost:8080/prometheus/api/v1/read"
	tenantID               = "10428"
	histogramBasesFilepath = "/Users/leizor/tmp/science/20240409/histogram_bases.out"
)

func main() {
	histogramBases, err := readHistogramBases()
	if err != nil {
		panic(err)
	}

	client, err := initializeReadClient()
	if err != nil {
		panic(err)
	}

	now := time.Now().Truncate(time.Hour)

	for _, hb := range histogramBases {
		// Start at the 12-hour mark and go backwards from there to avoid querying against the ingesters.
		for i := 12; i < 24; i++ {
			hourOffset := time.Duration((i+2)*-1) * time.Hour
			start := now.Add(hourOffset)

			ts, err := checkHistogram(client, hb, start)
			if err != nil {
				panic(err)
			}

			if ts > 0 {
				fmt.Printf("Found! %s at %s\n", hb, time.UnixMilli(ts).UTC().String())

				fmt.Printf(`sum({__aggregation__="%s_bucket:sum:counter",le="+Inf"})`, hb)
				fmt.Println()
				fmt.Printf(`sum({__aggregation__="%s_count:sum:counter"})`, hb)
				fmt.Println()
				fmt.Printf("Start: %s, End: %s\n", start.UTC().String(), start.Add(time.Hour).UTC().String())
				fmt.Println()
			}
		}
	}
}

func readHistogramBases() ([]string, error) {
	f, err := os.Open(histogramBasesFilepath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var histogramBases []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		hb := scanner.Text()
		histogramBases = append(histogramBases, hb)
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}

	return histogramBases, nil
}

func initializeReadClient() (remote.ReadClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	return remote.NewReadClient("foobar", &remote.ClientConfig{
		URL: &commonconfig.URL{URL: u},
		Headers: map[string]string{
			"x-scope-orgid": tenantID,
		},
		Timeout:          model.Duration(5 * time.Minute),
		ChunkedReadLimit: config.DefaultChunkedReadLimit,
	})
}

func checkHistogram(client remote.ReadClient, histogramBase string, hourStart time.Time) (int64, error) {
	startTimestampMs := hourStart.UnixMilli()
	endTimestampMs := hourStart.Add(time.Hour).UnixMilli()

	queries := []*prompb.Query{
		{
			StartTimestampMs: startTimestampMs,
			EndTimestampMs:   endTimestampMs,
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__aggregation__",
					Value: fmt.Sprintf("%s_count:sum:counter", histogramBase),
				},
			},
		},
		{
			StartTimestampMs: startTimestampMs,
			EndTimestampMs:   endTimestampMs,
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__aggregation__",
					Value: fmt.Sprintf("%s_bucket:sum:counter", histogramBase),
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "le",
					Value: "+Inf",
				},
			},
		},
	}

	res := []map[int64]float64{
		make(map[int64]float64),
		make(map[int64]float64),
	}

	for i, q := range queries {
		ss, err := client.Read(context.Background(), q, false)
		if err != nil {
			return -1, err
		}

		for ss.Next() {
			if err = ss.Err(); err != nil {
				return -1, err
			}

			series := ss.At()
			it := series.Iterator(nil)

			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				if err = it.Err(); err != nil {
					return -1, fmt.Errorf("error iterating through %v: %w", series.Labels(), err)
				}
				if vt != chunkenc.ValFloat {
					return -1, fmt.Errorf("expected float result for query '%v', got %s", q, vt.String())
				}

				ts, v := it.At()

				if _, ok := res[i][ts]; !ok {
					res[i][ts] = v
				} else {
					res[i][ts] += v
				}
			}
		}
	}

	if len(res[0]) >= len(res[1]) {
		for ts, v0 := range res[0] {
			v1 := res[1][ts]
			if v0 != v1 {
				return ts, nil
			}
		}
	} else {
		for ts, v0 := range res[1] {
			v1 := res[0][ts]
			if v0 != v1 {
				return ts, nil
			}
		}
	}

	return 0, nil
}
