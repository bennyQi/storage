package main

import (
	"sync"

	"github.com/google/btree"
)

type eleKV struct {
	K uint64
}

func (p eleKV) Less(q btree.Item) bool {
	return p.K < q.(eleKV).K
}

type mrow struct {
	sync.RWMutex
	Ref uint64
	*btree.BTree
	item    eleKV
	readers int
}

func (r *mrow) add(params ...uint64) {
	r.Lock()
	for _, param := range params {
		r.item.K = param
		r.ReplaceOrInsert(r.item)
	}
	r.Unlock()
}

func (r *mrow) match(params ...uint64) bool {
	r.Lock()
	for _, param := range params {
		r.item.K = param
		if n := r.Get(r.item); n == nil {
			return false
		}
	}
	r.Unlock()
	r.readers++
	return true

}

type CC struct {
	sync.Mutex
	sync.WaitGroup
}
type Matrix struct {
	sync.RWMutex
	table map[uint64]*mrow
	seq   []*mrow
	conCh chan *CC
	step  int
	con   int
	wg    sync.WaitGroup
}

func NewMatrix(step int, con int) *Matrix {
	mat := &Matrix{}
	mat.step = step
	mat.con = con
	mat.conCh = make(chan *CC, mat.con)
	for i := 0; i < mat.con; i++ {
		mat.conCh <- &CC{}
	}
	return mat
}
func (mat *Matrix) Add(ref uint64, lps ...uint64) {
	mat.Lock()
	row, ok := mat.table[ref]
	if !ok {
		row = &mrow{BTree: btree.New(8), Ref: ref}
		mat.table[ref] = row
		mat.seq = append(mat.seq, row)
	}
	mat.Unlock()

	row.add(lps...)
	return
}
func (l *Matrix) Query(params ...uint64) []uint64 {
	l.RLock()
	defer l.RUnlock()

	cc := <-l.conCh

	var refs []uint64
	for i, it := 0, 0; i < len(l.seq); it++ {
		f := i
		r := f + l.step - 1
		if r >= len(l.seq) {
			r = len(l.seq) - 1
		}
		i = r + 1
		cc.Add(1)
		go func(idx, first, last int) {
			subrefs := l.partiton(idx, first, last, params...)
			cc.Lock()
			refs = append(refs, subrefs...)
			cc.Unlock()
			l.wg.Done()
		}(it, f, r)
	}
	cc.Wait()

	l.conCh <- cc
	return refs
}
func (l *Matrix) partiton(i int, first, last int, params ...uint64) []uint64 {
	var refs []uint64
	for _, r := range l.seq[first:last] {
		if r.match(params...) {
			refs = append(refs, r.Ref)
		}
	}
	return refs
}
