package main

import (
	"time"

	"github.com/kbuci/text-hosting-mock/jobqueue"
)

func main() {
	time.Sleep(time.Second * 40)
	consumer := jobqueue.NewConsumer()
	consumer.ListenJobs()
}
