package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/adjust/rmq/v3"
)

var (
	respCh        = make(chan string)
	requestQueue  rmq.Queue
	responseQueue rmq.Queue
	respMap       = make(map[uint64]string)
	sequenceMutex sync.Mutex
)

func initQueue() {
	host := os.Getenv("REDIS_MASTER_SERVICE_HOST")

	connection, err := rmq.OpenConnection("service", "tcp", host+":6379", 1, nil)

	if err != nil {
		panic(err)
	}
	requestQueue, err = connection.OpenQueue("request")

	if err != nil {
		panic(err)
	}

	responseQueue, err = connection.OpenQueue("response")

	if err != nil {
		panic(err)
	}

	requestQueue.StartConsuming(10000, time.Second)

	requestConsumer := &Consumer{
		name:   fmt.Sprintf("consumer"),
		count:  0,
		before: time.Now(),
	}
	requestQueue.AddConsumer("request consumer", requestConsumer)
}

type Consumer struct {
	name   string
	count  int
	before time.Time
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	consumer.count++

	payload := delivery.Payload()

	fmt.Printf("payload : %s\n", payload)

	if index := strings.IndexByte(payload, '{'); index != -1 {
		header := payload[:index]
		body := payload[index:]
		responseQueue.Publish(header + body)
		delivery.Ack()
	}
}

func main() {

	fmt.Printf("consumer starting......\n")
	initQueue()

	select {}
}
