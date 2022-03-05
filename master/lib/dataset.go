package lib

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/thezzisu/bltrader/common"
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

func LoadDatasetInt32Async(stock, chunk int, database string) <-chan []int32 {
	r := make(chan []int32)
	go func() {
		defer close(r)
		r <- loadDatasetInt32(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
	}()
	return r
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

func LoadDatasetFloat64Async(stock, chunk int, database string) <-chan []float64 {
	r := make(chan []float64)
	go func() {
		defer close(r)
		r <- loadDataSetFloat64(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
	}()
	return r
}

func LoadOrderChunk(stock, chunk int) []common.BLOrder {
	idsCh := LoadDatasetInt32Async(stock, chunk, "order_id")
	directionsCh := LoadDatasetInt32Async(stock, chunk, "direction")
	typesCh := LoadDatasetInt32Async(stock, chunk, "type")
	pricesCh := LoadDatasetFloat64Async(stock, chunk, "price")
	volumesCh := LoadDatasetInt32Async(stock, chunk, "volume")
	ids, directions, types, prices, volumes := <-idsCh, <-directionsCh, <-typesCh, <-pricesCh, <-volumesCh

	n := len(ids)
	items := make([]common.BLOrder, n)
	for i := 0; i < n; i++ {
		order := common.BLOrder{
			StkCode:   int32(stock),
			OrderId:   ids[i],
			Direction: directions[i],
			Type:      types[i],
			Price:     prices[i],
			Volume:    volumes[i],
		}
		items[i] = order
	}
	return items
}

func CheckCache() {
	_, err := os.Stat(path.Join(Config.CacheDir, "stage-1", ".lock"))
	if err != nil {
		Logger.Fatalln(err)
	}
}
