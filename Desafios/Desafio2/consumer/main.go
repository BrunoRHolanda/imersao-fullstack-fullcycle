package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	KafkaServer string = "kafka:9092"
	kafkaConsumerGroupId string = "challenge"
)

func newConsumer() *kafka.Consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": KafkaServer,
		"group.id":          kafkaConsumerGroupId,
		"auto.offset.reset": "earliest",
	}
	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	return c
}

func processMessage(msg *kafka.Message) {
	challengeTopic := "challenge-two"

	switch topic := *msg.TopicPartition.Topic; topic {
	case challengeTopic:
		fmt.Println(string(msg.Value))
	default:
		fmt.Println("not a valid topic ", string(msg.Value))
	}
}

func main() {
	c := newConsumer()

	topic := []string{"challenge-two"}

	c.SubscribeTopics(topic, nil)

	fmt.Println("kafka consumer has been started")

	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			processMessage(msg)
		}
	}
}
