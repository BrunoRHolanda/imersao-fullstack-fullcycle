package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	KafkaServer string = "kafka:9092"
)

func newProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": KafkaServer,
	}
	p, err := kafka.NewProducer(configMap)

	if err != nil {
		panic(err)
	}

	return p
}

func deliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Delivery failed:", ev.TopicPartition)
			} else {
				fmt.Println("Delivered message to:", ev.TopicPartition)
			}
		}
	}
}

func main() {
	p := newProducer()
	deliveryChan := make(chan kafka.Event)
	topic := "challenge-two"

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte("Publish msg in topic challenge two"),
	}

	err := p.Produce(message, deliveryChan)

	if err != nil {
		fmt.Errorf("an error ocurred: %s", err.Error())
	}

	deliveryReport(deliveryChan)
}
