package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"GoFlatDB"
)

type TestData struct {
	Foo string `json:"foo"`
}

func main() {
	durF := flag.Duration("duration", 5*time.Second, "duration of test run time")
	workersF := flag.Int("workers", 100, "number of concurrent workers")

	flag.Parse()

	dir, err := os.MkdirTemp("", "db-bench")
	if err != nil {
		panic(err)
	}
	logger, _ := zap.NewProduction()
	db, err := GoFlatDB.NewFlatDB(dir, logger)
	if err != nil {
		panic(err)
	}
	col, err := GoFlatDB.NewFlatDBCollection[TestData](db, "bench", logger)
	if err != nil {
		panic(err)
	}
	if err := col.Init(); err != nil {
		panic(err)
	}

	start := time.Now()
	end := start.Add(*durF)

	wg := sync.WaitGroup{}
	for i := 0; i < *workersF; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for time.Now().Before(end) {
				_, err := col.Insert(&TestData{Foo: "hello world"})
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	wg.Wait()

	numRecords, err := os.ReadDir(filepath.Join(dir, "bench"))
	if err != nil {
		panic(err)
	}

	fmt.Println("inserted ", len(numRecords), "records")
	fmt.Println("qps ", float64(len(numRecords))/durF.Seconds(), "records")
}
