package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adjust/rmq/v3"
)

var (
	//respCh         = make(chan string)
	requestQueue   rmq.Queue
	responseQueue  rmq.Queue
	respMap        = make(map[uint64](chan string))
	sequenceNumber uint64
	sequenceMutex  sync.Mutex
	mapMutex       sync.Mutex
)

func index(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)

	sequenceMutex.Lock()
	sequenceNumber++
	sequenceMutex.Unlock()

	fmt.Printf("sequence : %d\n", sequenceNumber)
	localSequence := sequenceNumber
	respMap[localSequence] = make(chan string)
	requestQueue.Publish(os.Getenv("HOSTNAME") + "|" + strconv.FormatUint(sequenceNumber, 10) + string(body))

	resp := <-respMap[localSequence]
	w.Write([]byte(resp))
	delete(respMap, localSequence)
	return
}

func init() {
	sequenceNumber = 0
}

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

	responseQueue.StartConsuming(10, time.Second)

	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("consumer%d", i)
		batchConsumer := &BatchConsumer{name}

		responseQueue.AddBatchConsumer("response", 100, time.Millisecond, batchConsumer)
	}

	go func() {
		for range time.Tick(time.Millisecond) {
			responseQueue.ReturnRejected(math.MaxInt64)
		}
	}()

	go func() {
		cleaner := rmq.NewCleaner(connection)

		for range time.Tick(time.Minute) {
			returned, err := cleaner.Clean()
			if err != nil {
				log.Printf("failed to clean: %s", err)
				continue
			}
			log.Printf("cleaned %d", returned)
		}
	}()
}

type BatchConsumer struct {
	tag string
}

type Consumer struct {
	name   string
	count  int
	before time.Time
}

func (consumer *BatchConsumer) Consume(deliveries rmq.Deliveries) {

	for _, delivery := range deliveries {
		payload := delivery.Payload()
		log.Printf("consuming..... : %s\n", payload)

		if index := strings.IndexByte(payload, '{'); index != -1 {
			header := payload[:index]
			body := payload[index:]

			log.Printf("header : %s\n", header)
			log.Printf("body : %s\n", body)

			headers := strings.Split(header, "|")

			if headers[0] != os.Getenv("HOSTNAME") {
				log.Printf("reject!!")
				delivery.Reject()
				continue
			}

			sequence, err := strconv.ParseUint(string(headers[1]), 10, 64)

			if err == nil {
				mapMutex.Lock()
				respMap[sequence] <- body
				mapMutex.Unlock()
			}

			delivery.Ack()
		}
	}
}

func main() {
	http.HandleFunc("/", index)

	fmt.Printf("New Server starting... \n")

	initQueue()

	http.ListenAndServe(":3000", nil)
}
