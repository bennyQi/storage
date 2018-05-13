package main

import (
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/pkg/errors"
)

var (
	ErrSeriesNotFound = errors.New("Series not found.")
	ErrNoSeriesLeft   = errors.New("No serieses left in result")
)

type Static struct {
	Time  int64
	Value float64
}
type SeriesMeta struct {
	Ref        uint64
	LabelPairs []string
	Statics    []Static //min,max,sum,avg
	Count      int64
	Offset     int64
	Internal   int64
	StartDate  int64
	EndDate    int64
}

func (s *SeriesMeta) UpdateStatic(t int64, v float64) {
	if s.Statics[0].Value > v {
		s.Statics[0].Time, s.Statics[0].Value = t, v
	}
	if s.Statics[1].Value < v {
		s.Statics[1].Time, s.Statics[1].Value = t, v
	}
	//==============
	s.Statics[2].Time, s.Statics[2].Value = t, s.Statics[2].Value+v
	s.Count = s.Count + 1
	s.Statics[3].Time, s.Statics[3].Value = t, s.Statics[2].Value/float64(s.Count)
}

type LabelPairsRef map[string]uint64
type LabelRef map[uint64]string
type RefLabel map[string]uint64
type Label2Series map[uint64][]uint64

type TimeValue struct {
	Time  int64
	Value float64
}
type Series struct {
	Pairs []TimeValue
}

type eleKV struct {
	K uint64
}

func (p eleKV) Less(q btree.Item) bool {
	return p.K < q.(eleKV).K
}

//type for mem
type Row struct {
	Ref uint64
	*btree.BTree
}
type Matrix struct {
	sync.RWMutex
	table  map[uint64]*Row
	seq    []*Row
	item   []eleKV
	thread int
	wg     sync.WaitGroup
}

func (l *Matrix) Insert(ref uint64, lps ...uint64) {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.table[ref]; ok {
		return
	} else {
		l.table[ref] = &Row{BTree: btree.New(8), Ref: ref}
		for _, lp := range lps {
			l.item[0].K = lp
			l.table[ref].ReplaceOrInsert(l.item[0])
		}
		l.seq = append(l.seq, l.table[ref])
	}
	return
}
func (l *Matrix) Lookup(params ...uint64) []uint64 {
	var refs []uint64
	for i, it := 0, 0; i < len(l.seq); it++ {
		f := i
		r := f + l.thread - 1
		if r >= len(l.seq) {
			r = len(l.seq) - 1
		}
		i = r + 1
		l.wg.Add(1)
		go func(idx, first, last int) {
			tses := l.lookup(idx, first, last, params...)
			l.Lock()
			refs = append(refs, tses...)
			l.Unlock()
			l.wg.Done()
		}(it, f, r)

	}
	l.wg.Wait()
	return refs
}
func (l *Matrix) lookup(i int, first, last int, params ...uint64) []uint64 {
	var tses []uint64
	for _, r := range l.seq[first:last] {
		have := true
		for _, parama := range params {
			l.item[i].K = parama
			if n := r.Get(l.item[i]); n == nil {
				have = false
				break
			}
		}
		if have {
			tses = append(tses, r.Ref)
		}
	}
	return tses
}

type DB struct {
	Mat       *Matrix
	Meta      map[uint64]*SeriesMeta
	Serieses  map[uint64]*Series
	LPR       LabelPairsRef
	LR        LabelRef
	L2S       Label2Series
	LifeCycle int64
	Dir       string
}

func (d *DB) Check() {
	fmt.Println(len(d.Serieses))
	for _, series := range d.Serieses {
		fmt.Println(series)
	}
}
func OpenDB(dir string, lifeCycle int64) *DB {
	if dir == "" {
		dir = "./"
	}
	if lifeCycle == 0 {
		lifeCycle = 3600
	}
	mat := &Matrix{
		table:  make(map[uint64]*Row),
		thread: 100,
		item:   make([]eleKV, 10000, 10000),
	}
	db := &DB{
		Mat:      mat,
		Meta:     make(map[uint64]*SeriesMeta),
		Serieses: make(map[uint64]*Series),
		LPR:      make(map[string]uint64),
		LR:       make(map[uint64]string),
		L2S:      make(map[uint64][]uint64),
	}
	return db
}

func (d *DB) Append(samples []Sample) (fast uint64, err error) {
	for _, s := range samples {
		var labels Labels
		if s.Fast == 0 {
			labels = FromMap(s.Lables)
			s.Fast = labels.Hash()
		}
		if sm, ok := d.Meta[s.Fast]; ok {
			sm.UpdateStatic(s.Date, s.Value)
		} else {
			sm := &SeriesMeta{Statics: make([]Static, 4, 4)}
			sm.UpdateStatic(s.Date, s.Value)
			hashes := labels.Hashes()
			d.Mat.Insert(s.Fast, hashes...)
		}
		if series, ok := d.Serieses[s.Fast]; ok {
			series.Pairs = append(series.Pairs, TimeValue{Time: s.Date, Value: s.Value})
		} else {
			series := &Series{}
			series.Pairs = append(series.Pairs, TimeValue{Time: s.Date, Value: s.Value})
			d.Serieses[s.Fast] = series
		}
	}
	return fast, nil
}

type QueryResult struct {
	serieses []uint64
	at       int
	tmin     int64
	tmax     int64
	sync.Mutex
	db *DB
}

func (q *QueryResult) Next() (labelpairs []string, tvs []TimeValue, err error) {
	q.Lock()
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
	}

}
func (d *DB) Query(params map[string]string, tmin, tmax int64) (query *QueryResult, err error) {
	return nil, nil
}

//mem2disk

//disk2mem
