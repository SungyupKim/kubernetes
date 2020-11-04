package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/adjust/rmq"
)

var (
	respCh    = make(chan string)
	taskQueue rmq.Queue
)

func index(w http.ResponseWriter, r *http.Request) {
	go func() {
		body, _ := ioutil.ReadAll(r.Body)
		taskQueue.Publish(string(body))
	}()

	body := <-respCh
	w.Write([]byte(body))
}

func main() {
	http.HandleFunc("/", index)
	fmt.Println("Server starting... v2")

	host := os.Getenv("REDIS_MASTER_SERVICE_HOST")

	log.Printf("%s\n", host)

	connection := rmq.OpenConnection("service", "tcp", host+":6379", 1)

	taskQueue = connection.OpenQueue("tasks")

	taskQueue.StartConsuming(10, time.Second)

	taskConsumer := &Consumer{
		name:   fmt.Sprintf("consumer"),
		count:  0,
		before: time.Now(),
	}
	taskQueue.AddConsumer("task consumer", taskConsumer)

	http.ListenAndServe(":3000", nil)
}

type Consumer struct {
	name   string
	count  int
	before time.Time
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	consumer.count++
	log.Printf("%s\n", delivery.Payload())
	respCh <- delivery.Payload()
}
