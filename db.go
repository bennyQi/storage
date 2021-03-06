package main

import (
	"fmt"
	"sync"
)

type DB struct {
	Mat        *Matrix
	Meta       map[uint64]*SeriesMeta
	Serieses   map[uint64]*Series
	LabelTable *LabelTable
	LifeCycle  int64
	Dir        string
	sync.Mutex
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
		Mat:        mat,
		Meta:       make(map[uint64]*SeriesMeta),
		Serieses:   make(map[uint64]*Series),
		LabelTable: NewLabelTable(),
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
			relations := d.LabelTable.Add(s.Lables)
			sm := &SeriesMeta{Statics: make([]Static, 4, 4), Relations: relations}
			sm.UpdateStatic(s.Date, s.Value)
			d.Mat.Add(s.Fast, relations...)
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
