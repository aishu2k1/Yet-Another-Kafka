package main

import (
	"sync"
	p "Kafka/producer"
)

var wg sync.WaitGroup

func main() {
	wg.Add(1)
	go p.Producer()
	wg.Wait()
}