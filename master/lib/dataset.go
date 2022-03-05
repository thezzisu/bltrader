package lib

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"
)

func loadDatasetInt32(path string) []int32 {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(f)
	var n uint32
	binary.Read(reader, binary.LittleEndian, &n)
	dataset := make([]int32, n)
	for i := uint32(0); i < n; i++ {
		binary.Read(reader, binary.LittleEndian, &dataset[i])
	}
	return dataset
}

func LoadDatasetInt32(stock, chunk int, database string) []int32 {
	return loadDatasetInt32(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
}

func loadDataSetFloat64(path string) []float64 {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(f)
	var n uint32
	binary.Read(reader, binary.LittleEndian, &n)
	dataset := make([]float64, n)
	for i := uint32(0); i < n; i++ {
		binary.Read(reader, binary.LittleEndian, &dataset[i])
	}
	return dataset
}

func LoadDatasetFloat64(stock, chunk int, database string) []float64 {
	return loadDataSetFloat64(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
}

func CheckCache() {
	_, err := os.Stat(path.Join(Config.CacheDir, "stage-1", ".lock"))
	if err != nil {
		Logger.Fatalln(err)
	}
}
