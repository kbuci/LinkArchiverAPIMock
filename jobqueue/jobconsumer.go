package jobqueue

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/kbuci/text-hosting-mock/dataadapter"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type JobConsumer struct {
	Consumer *kafka.Consumer
	Adapter  *dataadapter.DataAdapter
}

func NewConsumer() *JobConsumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka",
		"group.id":          "defaultGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	consumer.SubscribeTopics([]string{LoadLinkTopic}, nil)
	return &JobConsumer{consumer, dataadapter.NewDataAdapter()}
}

func (c *JobConsumer) ConsumeLinkJob(message []byte) error {
	var linkJob LinkCopyData
	err := json.Unmarshal(message, &linkJob)
	if err != nil {
		panic(err)
	}
	archive_location, err := archiveFile(&linkJob)
	if err != nil {
		fmt.Printf("File archiving error: %s (%v)\n", linkJob.Link, err)
		return c.Adapter.UpdateLinkUploaded(linkJob.Id, linkJob.Link, "Failed", dataadapter.FailedUpload)
	}
	return c.Adapter.UpdateLinkUploaded(linkJob.Id, linkJob.Link, archive_location, dataadapter.SuccessfulUpload)
}

func (c *JobConsumer) JobProcessor(msgQueue <-chan *kafka.Message) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range msgQueue {
			err := c.ConsumeLinkJob(msg.Value)
			if err != nil {
				fmt.Printf("Consumer Update value error: %v (%v)\n", err, msg)
			}
		}

	}()

	return &wg
}

func (c *JobConsumer) ListenJobs() {
	msgQueue := make(chan *kafka.Message)
	barrier := c.JobProcessor(msgQueue)
	for {
		msg, err := c.Consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			msgQueue <- msg
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			close(msgQueue)
			break
		}
	}
	barrier.Wait()
	c.Consumer.Close()
}
