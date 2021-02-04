package kafka

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
)

type Processor struct {
	Database *gorm.DB
	Producer *ckafka.Producer
	DeliveryChan chan ckafka.Event
}

func NewProcessor(database *gorm.DB, producer *ckafka.Producer, deliveryChan chan ckafka.Event) *Processor {
	return &Processor{
		Database:     database,
		Producer:     producer,
		DeliveryChan: deliveryChan,
	}
}

func (p *Processor) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"group.id": "consumergroup",
		"auto.offset.reset": "earliest",
	}
	c, err := ckafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	topics := []string{"test"}

	c.SubscribeTopics(topics, nil)

	fmt.Println("kafka consumer has been started")

	for {
		msg, err := c.ReadMessage(-1)

		if err == nil {
			fmt.Println(string(msg.Value))
		}
	}
}
