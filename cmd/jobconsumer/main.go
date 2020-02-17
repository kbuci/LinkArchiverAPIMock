package main

import (
	"time"

	"github.com/kbuci/multiuser-weblink-store/jobqueue"
)

func main() {
	time.Sleep(time.Second * 40)
	consumer := jobqueue.NewConsumer()
	consumer.ListenJobs()
}
