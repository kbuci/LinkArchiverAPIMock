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

func (c *JobConsumer) ArchiveLinkJob(message []byte) error {
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

func (c *JobConsumer) JobProcessor(workers int, msgQueue <-chan *kafka.Message) *sync.WaitGroup {
	var jobGroup sync.WaitGroup
	jobGroup.Add(workers)
	markConsumedQueue := make(chan *kafka.Message)
	for i := 0; i < workers; i++ {
		go func() {
			defer jobGroup.Done()
			for msg := range msgQueue {
				err := c.ArchiveLinkJob(msg.Value)
				if err != nil {
					fmt.Printf("Consumer Update value error: %v (%v)\n", err, msg)
				}
				markConsumedQueue <- msg
			}

		}()
	}

	go func() {
		jobGroup.Wait()
		close(markConsumedQueue)
	}()
	// Needed to enforce at least once processing and also avoid multiple threads commiting messages
	// at the same time. Using a channel/job queue is a straightforward approach to
	// allow committing the message and processing the next job to happen concurrently
	var markConsumedGroup sync.WaitGroup
	markConsumedGroup.Add(1)
	go func() {
		defer markConsumedGroup.Done()
		for msg := range markConsumedQueue {
			c.Consumer.CommitMessage(msg)
		}
	}()

	return &markConsumedGroup
}

func (c *JobConsumer) ListenJobs() {
	msgQueue := make(chan *kafka.Message)
	saveConsumedJobsWorker := c.JobProcessor(1, msgQueue)

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
	saveConsumedJobsWorker.Wait()
	c.Consumer.Close()
}
