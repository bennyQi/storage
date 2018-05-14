package main

import (
	"fmt"
	//"sync"
	//"github.com/pkg/errors"
)

type LabelPairsRef map[string]uint64
type LabelRef map[uint64]string
type RefLabel map[string]uint64
type Label2Series map[uint64][]uint64
type FastReg map[uint64]struct{}

type DB struct {
	Mat       *Matrix
	Meta      map[uint64]*SeriesMeta
	Serieses  map[uint64]*Series
	LPR       LabelPairsRef
	LR        LabelRef
	L2S       Label2Series
	FR        FastReg
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
	mat := NewMatrix(16, 10)
	db := &DB{
		Mat:      mat,
		Meta:     make(map[uint64]*SeriesMeta),
		Serieses: make(map[uint64]*Series),
		LPR:      make(map[string]uint64),
		LR:       make(map[uint64]string),
		L2S:      make(map[uint64][]uint64),
		FR:       make(map[uint64]struct{}),
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
		fast = s.Fast
		if sm, ok := d.Meta[s.Fast]; ok {
			sm.UpdateStatic(s.Date, s.Value)
		} else {
			sm := &SeriesMeta{Statics: make([]Static, 4, 4)}
			sm.UpdateStatic(s.Date, s.Value)
			hashes := labels.Hashes()
			d.Mat.Add(s.Fast, hashes...)
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
func (d *DB) Close()  {}
func (d *DB) Commit() {}
func (d *DB) Query(params map[string]string, tmin, tmax int64) (query *QueryResult, err error) {
	return nil, nil
}

//mem2disk

//disk2mem
