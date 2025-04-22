package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/DemoSwDeveloper/pigeon-go/internal/middleware/logging"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/nsqio/go-nsq"
)

const (
	RequestsChannel = "requests"
	EmailsChannel   = "emails"
	EmailsTopic     = "emails_requests"
)

func main() {
	log.Println("logs archive starting...")

	signalchan := make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGINT, syscall.SIGTERM)

	handlerFunc := nsq.HandlerFunc(func(message *nsq.Message) error {
		var messageID []byte
		copy(messageID, message.ID[:])
		var logEntry *model.LogEntry
		err := json.Unmarshal(message.Body, &logEntry)
		if err != nil {
			log.Printf("ID=%s, error: %v\n", string(messageID), err)
			return nil
		}
		log.Printf("ID=%s, %s\n", string(messageID), logEntry.String())
		return nil
	})

	consumer, err := nsq.NewConsumer(logging.NSQApiRequestTopic, RequestsChannel, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(handlerFunc)

	emailsConsumer, err := nsq.NewConsumer(EmailsTopic, EmailsChannel, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	emailsConsumer.AddHandler(handlerFunc)

	nsqLookupdAddress, found := os.LookupEnv("NSQLOOKUPD_ADDRESS")

	if !found {
		log.Fatal(errors.New("NSQLOOKUPD address has not been found"))
	}

	err = consumer.ConnectToNSQLookupds([]string{nsqLookupdAddress})
	if err != nil {
		log.Fatal(err)
	}

	err = emailsConsumer.ConnectToNSQLookupds([]string{nsqLookupdAddress})
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		consumer.Stop()
		emailsConsumer.Stop()
	}()

	log.Println("logs archive started")

	stop := <-signalchan

	log.Printf("logs archive stopped: %v\n", stop)
}
