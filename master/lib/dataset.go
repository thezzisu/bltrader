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

var lastPrev []float64
var ChunkCount int

func InitDataset() {
	lastPrev = loadDataSetFloat64(path.Join(Config.CacheDir, "stage-1", "last_prev.bin"))
	Logger.Printf("last prev loaded (%d)\n", len(lastPrev))
	ChunkCount = 0
	for {
		_, err := os.Stat(path.Join(Config.CacheDir, "stage-1", "0", fmt.Sprintf("order_id.%d.bin", ChunkCount)))
		if err != nil {
			break
		}
		ChunkCount++
	}
	Logger.Println("database chunk count:", ChunkCount)
}

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

func LoadDatasetInt32(stock int32, chunk int, database string) []int32 {
	return loadDatasetInt32(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
}

func LoadDatasetInt32Async(stock int32, chunk int, database string) <-chan []int32 {
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

func LoadDatasetFloat64(stock int32, chunk int, database string) []float64 {
	return loadDataSetFloat64(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
}

func LoadDatasetFloat64Async(stock int32, chunk int, database string) <-chan []float64 {
	r := make(chan []float64)
	go func() {
		defer close(r)
		r <- loadDataSetFloat64(path.Join(Config.CacheDir, "stage-1", fmt.Sprint(stock), fmt.Sprintf("%s.%d.bin", database, chunk)))
	}()
	return r
}

func LoadOrderChunk(stock int32, chunk int) []common.BLOrder {
	idsCh := LoadDatasetInt32Async(stock, chunk, "order_id")
	directionsCh := LoadDatasetInt32Async(stock, chunk, "direction")
	typesCh := LoadDatasetInt32Async(stock, chunk, "type")
	pricesCh := LoadDatasetFloat64Async(stock, chunk, "price")
	volumesCh := LoadDatasetInt32Async(stock, chunk, "volume")
	ids, directions, types, prices, volumes := <-idsCh, <-directionsCh, <-typesCh, <-pricesCh, <-volumesCh

	prev := lastPrev[stock]
	lower := prev * 0.9
	upper := prev * 1.1

	n := len(ids)
	items := make([]common.BLOrder, n)
	for i := 0; i < n; i++ {
		if types[i] == 0 && (prices[i] < lower || prices[i] > upper) {
			volumes[i] = 0
		}
		items[i] = common.BLOrder{
			StkCode:   int32(stock),
			OrderId:   ids[i],
			Direction: directions[i],
			Type:      types[i],
			Price:     prices[i],
			Volume:    volumes[i],
		}
	}
	return items
}

func LoadHooks(stock int32) []common.BLHook {
	f, err := os.Open(path.Join(Config.CacheDir, "stage-2", fmt.Sprintf("hook_%d.bin", stock)))
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(f)
	var n uint32
	binary.Read(reader, binary.LittleEndian, &n)
	dataset := make([]common.BLHook, n)
	for i := uint32(0); i < n; i++ {
		binary.Read(reader, binary.LittleEndian, &dataset[i].SelfOrderId)
		binary.Read(reader, binary.LittleEndian, &dataset[i].TargetStkCode)
		binary.Read(reader, binary.LittleEndian, &dataset[i].TargetTradeIdx)
		binary.Read(reader, binary.LittleEndian, &dataset[i].Arg)
	}
	return dataset
}

func CheckCache() {
	_, err := os.Stat(path.Join(Config.CacheDir, "stage-1", ".lock"))
	if err != nil {
		Logger.Fatalln(err)
	}
	_, err = os.Stat(path.Join(Config.CacheDir, "stage-2", ".lock"))
	if err != nil {
		Logger.Fatalln(err)
	}
}
