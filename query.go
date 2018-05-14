package main

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrSeriesNotFound = errors.New("Series not found.")
	ErrNoSeriesLeft   = errors.New("No serieses left in result")
)

type QueryResult struct {
	serieses []uint64
	at       int
	tmin     int64
	tmax     int64
	sync.Mutex
	db *DB
}

func (q *QueryResult) Next() (labelpairs []string, tvs []TimeValue, err error) {
	/*q.Lock()
	defer q.Unlock()
	for {
		if q.at+1 > len(q.serieses) {
			return nil, nil, ErrNoSeriesLeft
		}
		q.at = q.at + 1
		meta, ok := q.db.Meta[q.serieses[q.at]]
		if !ok {
			return nil, nil, ErrSeriesNotFound
		}
		series, ok := q.db.Serieses[q.serieses[q.at]]
		if !ok {
			return nil, nil, ErrSeriesNotFound
		}
		if meta.EndDate < q.tmin || meta.StartDate > q.tmax {
			continue
		} else {
			labelpairs = make([]string, len(meta.LabelPairs), len(meta.LabelPairs))
			copy(labelpairs, meta.LabelPairs)
			start := 0
			end := len(series.Pairs) - 1
			startDate := q.tmin
			if q.tmin-meta.StartDate > 0 {
				start = int((q.tmin - meta.StartDate) / meta.Internal)
				if start < 0 {
					start = 0
				}
				startDate = meta.StartDate
			}
			if q.tmax < meta.EndDate {
				tmpEnd := int((q.tmax-startDate)/meta.Internal + 1)
				if tmpEnd < end {
					end = tmpEnd
				}
			}
			tvs = series.Pairs[start:end]
		}
	}*/
	return nil, nil, nil

}

func (q *QueryResult) Close() {}
