package main

import ()

type SeriesState int

//Not always loaded series data once db open
const (
	S_NOTLOADED SeriesState = iota
	S_NEW
	S_LOADED
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
	s.Statics[2].Time, s.Statics[2].Value = t, s.Statics[2].Value+v
	s.Count = s.Count + 1
	s.Statics[3].Time, s.Statics[3].Value = t, s.Statics[2].Value/float64(s.Count)
}

type TimeValue struct {
	Time  int64
	Value float64
}
type Series struct {
	Pairs []TimeValue
}
