package kafka

import (
	"fmt"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/factory"
	appModel "github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/model"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/application/usecase"
	"github.com/BrunoRHolanda/imersao-fullstack-fullcycle/codepix/domain/model"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
	"os"
)

type Processor struct {
	Database     *gorm.DB
	Producer     *ckafka.Producer
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
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
		"group.id":          os.Getenv("kafkaConsumerGroupId"),
		"auto.offset.reset": "earliest",
	}
	c, err := ckafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	topics := []string{os.Getenv("kafkaTransactionTopic"), os.Getenv("kafkaTransactionConfirmationTopic")}

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
		p.processTransaction(msg)
	case transactionConfirmationTopic:
		p.processTransactionConfirmation(msg)
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

	createdTransaction, err := transactionUseCase.Register(
		transaction.AccountID,
		transaction.Amount,
		transaction.PixKeyTo,
		transaction.PixKeyKindTo,
		transaction.Description,
	)

	if err != nil {
		fmt.Println("error registering transaction", err)
	}

	topic := "bank"+createdTransaction.PixKeyTo.Account.Bank.Code
	transaction.ID = createdTransaction.ID
	transaction.Status = model.TransactionPending

	transactionJson, err := transaction.ToJson()

	if err != nil {
		return err
	}

	err = Publish(string(transactionJson), topic, p.Producer, p.DeliveryChan)

	if err != nil {
		return err
	}

	return nil
}

func (p *Processor) processTransactionConfirmation(msg *ckafka.Message) error {
	transaction := appModel.NewTransaction()
	err := transaction.ParseJson(msg.Value)
	if err != nil {
		return err
	}

	transactionUseCase := factory.TransactionUseCaseFactory(p.Database)

	if transaction.Status == model.TransactionConfirmed {
		err = p.confirmTransaction(transaction, transactionUseCase)
		if err != nil {
			return err
		}
	} else if transaction.Status == model.TransactionCompleted {
		_, err := transactionUseCase.Complete(transaction.ID)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (p *Processor) confirmTransaction(transaction *appModel.Transaction, transactionUseCase usecase.TransactionUseCase) error {
	confirmedTransaction, err := transactionUseCase.Confirm(transaction.ID)
	if err != nil {
		return err
	}

	topic := "bank" + confirmedTransaction.AccountFrom.Bank.Code
	transactionJson, err := transaction.ToJson()
	if err != nil {
		return err
	}

	err = Publish(string(transactionJson), topic, p.Producer, p.DeliveryChan)
	if err != nil {
		return err
	}
	return nil
}
