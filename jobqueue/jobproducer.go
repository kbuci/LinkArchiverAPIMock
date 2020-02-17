package jobqueue

import (
	"encoding/json"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type JobProducer struct {
	Producer *kafka.Producer
}

var LoadLinkTopic string = "LoadLink"
var DelayLinkTopic string = "DelayLoadLink"

type LinkCopyData struct {
	Link string
	Id   uint64
}

type LinkDelayData struct {
	Link          string
	Id            uint64
	WaitUntilTime int64
}

func NewProducer() *JobProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka"})
	if err != nil {
		panic(err)
	}
	return &JobProducer{producer}
}

func (p *JobProducer) QueueLinkCopyJob(id uint64, link string) error {
	message := LinkCopyData{
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

func (p *JobProducer) QueueLinkDelayJob(id uint64, link string, waitUntil int64) error {
	message := LinkDelayData{
		link,
		id,
		waitUntil,
	}

	//partition := int32(id)
	jsonMessage, _ := json.Marshal(message)
	return p.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &DelayLinkTopic, Partition: kafka.PartitionAny},
		Value:          jsonMessage,
	}, nil)
}
