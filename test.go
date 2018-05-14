package main

import (
	"compress/gzip"
	"fmt"
	"math/rand"
	"os"
	"time"
)

//easyjson:json
type TestDataSet struct {
	TSes  []*TestSeries
	Count int
}

//easyjson:json
type TestSeries struct {
	Labels map[string]string
	Rs     []uint64
	TVs    []TimeValue
	Ref    uint64
}

func init() {
	rand.Seed(time.Now().Unix())
}

var (
	lablesName = []string{"fd", "fdaf", "w3e", "ok30", "fdafafafa", "jfirjffmfjf", "e3errrt", "dkpdlfkfk09", "vmfkfmvjd", "ploo4nnhb", "cpu", "mem", "host", "ip", "reack", "city", "location", "ladn", "house", "chiar", "rail", "bed", "landuf", "flowere", "niew09io", "oirutyvnb"}
)

func GenDiskFile(batch, count int) {
	file, err := os.OpenFile("test.json", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	tds := &TestDataSet{}
	lt := &LabelTable{}
	lt.LabelValue = &LabelValue{
		T2I: make(map[string]uint64),
		I2T: make(map[uint64]string),
	}
	lt.Relations = &Relations{
		R2I: make(map[string]uint64),
		I2R: make(map[uint64]*Relation),
	}

	for i := 0; i < batch; i++ {
		ts := &TestSeries{}
		labels := make(map[string]string)
		for j := 0; j < 13; j++ {
			ni := rand.Intn(len(lablesName))
			name := lablesName[ni]
			vi := rand.Intn(len(lablesName))
			value := lablesName[vi]

			labels[name] = value
		}
		for name, value := range labels {
			nId := lt.LabelValue.GetIndex(name)
			vId := lt.LabelValue.GetIndex(value)
			rId := lt.Relations.GetIndex(nId, vId)
			ts.Rs = append(ts.Rs, rId)
		}
		tds.TSes = append(tds.TSes, ts)
	}

	data, _ := lt.MarshalJSON()
	newGzipWriter := gzip.NewWriter(file)
	newGzipWriter.Write(data)
	newGzipWriter.Close()
	file.Sync()
	//file.WriteString("\n")
	return
	for i := 0; i < batch; i++ {
		data, _ := tds.TSes[i].MarshalJSON()
		file.WriteString(string(data))
		file.WriteString("\n")
	}
	return
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
	tds := GenSamples(200000, 100000)
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
