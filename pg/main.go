package main

import (
	"fmt"
	"math"
)

func main() {
	fmt.Println("PG:")
	for i := 0; i < 1000000000; i++ {
		var a float64 = float64(i) / 100
		// fmt.Println(a)
		var b int32 = int32(math.Round(a * 100))
		// fmt.Println(b)
		var c float64 = float64(b) / 100
		// fmt.Println(c)
		if a != c {
			fmt.Println(i, a, b, c)
		}
	}
}
