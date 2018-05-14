package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

var (
	lablesName = []string{"fd", "fdaf", "w3e", "ok30", "fdafafafa", "jfirjffmfjf", "e3errrt", "dkpdlfkfk09", "vmfkfmvjd", "ploo4nnhb", "cpu", "mem", "host", "ip", "reack", "city", "location", "ladn", "house", "chiar", "rail", "bed", "landuf", "flowere", "niew09io", "oirutyvnb"}
)

type TestDataSet struct {
	TSes  []*TestSeries
	Count int
}
type TestSeries struct {
	Labels map[string]string
	TVs    []TimeValue
	Ref    uint64
}

func GenSamples(batch, count int) *TestDataSet {
	tds := &TestDataSet{}
	baseTime := time.Now().Unix()
	for i := 0; i < batch; i++ {
		ts := &TestSeries{}
		labels := make(map[string]string)
		for j := 0; j < 13; j++ {
			ni := rand.Intn(len(lablesName))
			name := lablesName[ni]
			value, _ := GenUUID()
			labels[name] = value
		}
		ts.Labels = labels
		tds.TSes = append(tds.TSes, ts)
	}

	for i := 0; i < count; i++ {
		for j := 0; j < batch; j++ {
			tv := TimeValue{}
			tv.Time = baseTime + int64(i)
			tv.Value = rand.Float64()
			tds.TSes[j].TVs = append(tds.TSes[j].TVs, tv)
		}
	}
	tds.Count = count
	return tds
}
func GenLookupTable() *DB {
	db := OpenDB("", 0)
	tds := GenSamples(50, 1000000)
	for i := 0; i < tds.Count; i++ {
		for j := 0; j < len(tds.TSes); j++ {
			var samples []Sample
			sample := Sample{
				Lables: tds.TSes[j].Labels,
				Date:   tds.TSes[j].TVs[i].Time,
				Value:  tds.TSes[j].TVs[i].Value,
				Fast:   tds.TSes[j].Ref,
			}
			samples = append(samples, sample)
			fast, err := db.Append(samples)
			if err != nil {
				fmt.Println("append failed", err)
				return nil
			}
			if tds.TSes[j].Ref == 0 {
				tds.TSes[j].Ref = fast
			}
		}
	}
	return db
}

func main() {
	s := time.Now().UnixNano()
	lt := GenLookupTable()
	fmt.Println((time.Now().UnixNano() - s) / 1000000)
	lt.Check()
	return
	for {
		fmt.Println(">>>>")
		inputReader := bufio.NewReader(os.Stdin)
		input, err := inputReader.ReadString('\n')
		if err == nil {
			fmt.Printf("The input was: %s\n", input)
		}
		s := time.Now().UnixNano()
		serieses := lt.Mat.Query(23, 56, 12, 8, 9, 12, 34, 49, 11, 4, 2, 3, 99, 12, 45, 39, 23, 54)
		fmt.Println((time.Now().UnixNano() - s) / 1000000)
		fmt.Println(len(serieses))
	}
}
