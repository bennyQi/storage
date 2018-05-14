package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func main() {
	GenDiskFile(200000, 100000)
	//GenDiskFile(20, 100)
	return
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
