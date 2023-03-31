package main

import (
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type sampleResult struct {
	metricName           string
	numObservations      uint64
	numSpans, numBuckets int
}

type chkResult struct {
	metricName  string
	chkEncoding chunkenc.Encoding
	sizeBytes   int
	numSamples  int
}

func (r *chkResult) write(f *os.File) error {
	s := fmt.Sprintf("%s\t%d\t%d\n", r.metricName, r.sizeBytes, r.numSamples)
	_, err := f.WriteString(s)
	return err
}

func main() {
	const (
		blockDir     = "/Users/leizor/tmp/science/prometheus/13611"
		intHistOut   = "/Users/leizor/tmp/inthist.out"
		floatHistOut = "/Users/leizor/tmp/floathist.out"
	)

	db, err := tsdb.OpenDBReadOnly(blockDir, log.NewNopLogger())
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	blockReaders, err := db.Blocks()
	if err != nil {
		panic(err)
	}

	intf, err := os.Create(intHistOut)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = intf.Close()
	}()
	floatf, err := os.Create(floatHistOut)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = floatf.Close()
	}()

	_, err = intf.WriteString("metric\tsizeBytes\tnumSamples\n")
	if err != nil {
		panic(err)
	}
	_, err = floatf.WriteString("metric\tsizeBytes\tnumSamples\n")
	if err != nil {
		panic(err)
	}

	for i, br := range blockReaders {
		ulid := br.Meta().ULID.String()
		if i%100 == 0 {
			fmt.Printf("Walking block %s (count: %d)...\n", ulid, i)
		}

		cRes, _, err := walkBlock(br)
		if err != nil {
			fmt.Println(fmt.Errorf("problem walking block %s: %v", ulid, err))
			os.Exit(1)
		}

		for _, r := range cRes {
			var f *os.File
			switch r.chkEncoding {
			case chunkenc.EncHistogram:
				f = intf
			case chunkenc.EncFloatHistogram:
				f = floatf
			default:
				panic("unrecognized chunk encoding: " + r.chkEncoding.String())
			}
			err := r.write(f)
			if err != nil {
				panic(err)
			}
		}
	}
}

func walkBlock(br tsdb.BlockReader) ([]chkResult, []sampleResult, error) {
	idx, err := br.Index()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = idx.Close()
	}()

	metricNames, err := idx.LabelValues("__name__")
	if err != nil {
		return nil, nil, err
	}

	postings, err := idx.Postings("__name__", metricNames...)
	if err != nil {
		return nil, nil, err
	}

	chkReader, err := br.Chunks()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = chkReader.Close()
	}()

	var (
		sRes []sampleResult
		cRes []chkResult
	)

	for postings.Next() {
		sRef := postings.At()

		var (
			builder labels.ScratchBuilder
			chks    []chunks.Meta
		)
		err := idx.Series(sRef, &builder, &chks)
		if err != nil {
			return nil, nil, err
		}

		metricName := builder.Labels().Get("__name__")

		for _, chkMeta := range chks {
			chk, err := chkReader.Chunk(chkMeta)
			if err != nil {
				return nil, nil, err
			}

			if chk.Encoding() == chunkenc.EncHistogram || chk.Encoding() == chunkenc.EncFloatHistogram {
				cRes = append(cRes, chkResult{
					metricName:  metricName,
					chkEncoding: chk.Encoding(),
					sizeBytes:   len(chk.Bytes()),
					numSamples:  chk.NumSamples(),
				})
			}

			//if chk.Encoding() == chunkenc.EncHistogram || chk.Encoding() == chunkenc.EncFloatHistogram {
			//	chk, err := chunkenc.FromData(chk.Encoding(), chk.Bytes())
			//	if err != nil {
			//		return nil, nil, err
			//	}
			//
			//	chkItr := chk.Iterator(nil)
			//	for valType := chkItr.Next(); valType != chunkenc.ValNone; valType = chkItr.Next() {
			//		res := sampleResult{metricName: metricName}
			//
			//		switch valType {
			//		case chunkenc.ValHistogram:
			//			_, hist := chkItr.AtHistogram()
			//			res.numObservations = hist.Count
			//			res.numSpans = len(hist.PositiveSpans) + len(hist.NegativeSpans)
			//			res.numBuckets = len(hist.PositiveBuckets) + len(hist.NegativeBuckets)
			//		case chunkenc.ValFloatHistogram:
			//			_, fhist := chkItr.AtFloatHistogram()
			//			res.numObservations = uint64(fhist.Count)
			//			res.numSpans = len(fhist.PositiveSpans) + len(fhist.NegativeSpans)
			//			res.numBuckets = len(fhist.PositiveBuckets) + len(fhist.NegativeBuckets)
			//		}
			//
			//		sRes = append(sRes, res)
			//	}
			//}
		}
	}

	return cRes, sRes, nil
}
