package kafka

import (
	"fmt"
	appModel "github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/model"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
)

type Processor struct {
	Database *gorm.DB
	Producer *ckafka.Producer
	DeliveryChan chan ckafka.Event
}

//pp
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
			p.processMessage(msg)
		}
	}
}

func (p *Processor) processMessage(msg *ckafka.Message) {
	transactionTopic := "transactions"
	transactionConfirmationTopic := "transaction_confirmation"

	switch topic := *msg.TopicPartition.Topic; topic {
	case transactionTopic:
	case transactionConfirmationTopic:
	default:
		fmt.Println("not a valid topic ", string(msg.Value))
	}
}

func (p *Processor) processTransaction(msg *ckafka.Message) error {
	transaction := appModel.NewTransaction()

	err := transaction.ParseJson(msg.Value)

	if err != nil {
		return err
	}

	transactionUseCase := factory.TransactionUseCaseFactory(p.Database)
}
