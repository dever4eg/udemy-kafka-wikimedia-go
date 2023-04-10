package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/r3labs/sse/v2"
	"github.com/segmentio/kafka-go"
)

func main() {
	godotenv.Load()

	log.Println("Wikimedia producer started")

	ctx := context.Background()

	url := os.Getenv("WIKIMEDIA_STREAM_URL")
	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	kafkaTopicWikimedia := os.Getenv("KAFKA_TOPIC_WIKIMEDIA")

	logFn := func(msg string, a ...interface{}) {
		log.Printf(msg, a...)
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(kafkaBootstrapServers, ",")...),
		Topic:                  kafkaTopicWikimedia,
		Balancer:               &kafka.RoundRobin{},
		RequiredAcks:           -1,
		AllowAutoTopicCreation: true,
		Async:                  true,
		Logger:                 kafka.LoggerFunc(logFn),
		ErrorLogger:            kafka.LoggerFunc(logFn),
	}

	client := sse.NewClient(url)
	events := make(chan *sse.Event)
	go client.SubscribeChanWithContext(ctx, "messages", events)

	for event := range events {
		log.Printf("Received a message from stream")

		writer.WriteMessages(ctx, kafka.Message{
			Value: event.Data,
		})
	}
}
