package jobqueue

import (
	"encoding/json"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type JobProducer struct {
	Producer *kafka.Producer
}

var LoadLinkTopic string = "LoadLink"

type LinkCopyData struct {
	Title string
	Link  string
	Id    uint64
}

func NewProducer() *JobProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka"})
	if err != nil {
		panic(err)
	}
	return &JobProducer{producer}
}

func (p *JobProducer) QueueLinkCopyJob(id uint64, title, link string) error {
	message := LinkCopyData{
		title,
		link,
		id,
	}

	//partition := int32(id)
	jsonMessage, _ := json.Marshal(message)
	return p.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &LoadLinkTopic, Partition: kafka.PartitionAny},
		Value:          jsonMessage,
	}, nil)
}
