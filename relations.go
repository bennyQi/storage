package main

import (
	"fmt"
	"sync"
)

//easyjson:json
type LabelValue struct {
	sync.RWMutex
	T2I   map[string]uint64
	I2T   map[uint64]string
	Index uint64
}

func (ll *LabelValue) GetIndex(text string) uint64 {
	ll.Lock()
	defer ll.Unlock()
	if index, ok := ll.T2I[text]; ok {
		return index
	}
	ll.Index = ll.Index + 1
	ll.T2I[text] = ll.Index
	ll.I2T[ll.Index] = text
	return ll.Index
}

func (ll *LabelValue) GetText(index uint64) (string, bool) {
	ll.RLock()
	text, ok := ll.I2T[index]
	ll.RUnlock()
	return text, ok
}

//easyjson:json
type Relation struct {
	N uint64
	V uint64
}

//easyjson:json
type Relations struct {
	sync.RWMutex
	R2I   map[string]uint64
	I2R   map[uint64]*Relation
	Index uint64
}

func (r *Relations) GetIndex(name, value uint64) uint64 {
	key := fmt.Sprintf("%d_%d", name, value)
	r.Lock()
	defer r.Unlock()
	if index, ok := r.R2I[key]; ok {
		return index
	}
	r.Index = r.Index + 1
	r.R2I[key] = r.Index
	relation := &Relation{N: name, V: value}
	r.I2R[r.Index] = relation
	return r.Index
}

func (r *Relations) GetRelation(index uint64) (name, value uint64, ok bool) {
	r.RLock()
	relation, ok := r.I2R[index]
	r.RUnlock()
	return relation.N, relation.V, ok
}

//easyjson:json
type LabelTable struct {
	LabelValue *LabelValue
	Relations  *Relations
}

func NewLabelTable() *LabelTable {
	lt := &LabelTable{}
	lt.LabelValue = &LabelValue{
		T2I: make(map[string]uint64),
		I2T: make(map[uint64]string),
	}
	lt.Relations = &Relations{
		R2I: make(map[string]uint64),
		I2R: make(map[uint64]*Relation),
	}
	return lt
}

func (lt *LabelTable) Add(labels map[string]string) []uint64 {
	var relations []uint64
	for n, v := range labels {
		ni := lt.LabelValue.GetIndex(n)
		vi := lt.LabelValue.GetIndex(v)
		relation := lt.Relations.GetIndex(ni, vi)
		relations = append(relations, relation)
	}
	return relations
}
