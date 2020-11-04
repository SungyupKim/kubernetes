package main

import (
	"net/http"
	"os"

	"github.com/adjust/rmq/v2"
	"github.com/pffreitas/rmqprom"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	//var connection rmq.Connection // get a RMQ Connection
	host := os.Getenv("REDIS_MASTER_SERVICE_HOST")
	connection := rmq.OpenConnection("service", "tcp", host+":6379", 1)
	rmqprom.RecordRmqMetrics(connection)

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
