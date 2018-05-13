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

func GenSamples(batch, count int) [][]Sample {
	baseTime := time.Now().Unix()
	HeaderSamples := make([]Sample, 0, 50)
	for i := 0; i < batch; i++ {
		sample := Sample{}
		sample.Lables = make(map[string]string)
		for j := 0; j < 13; j++ {
			ni := rand.Intn(len(lablesName))
			name := lablesName[ni]
			value, _ := GenUUID()
			sample.Lables[name] = value
		}
		HeaderSamples = append(HeaderSamples, sample)

	}
	var SamplesMat [][]Sample
	for i := 0; i < count; i++ {
		samplebatch := make([]Sample, 0, batch)
		for j := 0; j < batch; j++ {
			sample := Sample{}
			sample.Lables = HeaderSamples[j].Lables
			sample.Date = baseTime + int64(i)
			sample.Value = rand.Float64()
			samplebatch = append(samplebatch, sample)
		}
		SamplesMat = append(SamplesMat, samplebatch)

	}
	return SamplesMat
}
func GenLookupTable() *DB {
	db := OpenDB("", 0)
	smpMat := GenSamples(50, 1000000)
	for _, batch := range smpMat {
		db.Append(batch)
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
		serieses := lt.Mat.Lookup(23, 56, 12, 8, 9, 12, 34, 49, 11, 4, 2, 3, 99, 12, 45, 39, 23, 54)
		fmt.Println((time.Now().UnixNano() - s) / 1000000)
		fmt.Println(len(serieses))
	}
}
