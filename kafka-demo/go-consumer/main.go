package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	broker := "localhost:9092"
	groupID := "my-consumer-group"
	topic := "withdraw-money-topic"

	// Create consumer configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest", // Start from the beginning if no offset exists
	}

	// Create a new Kafka consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// Subscribe to the topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topics: %v\n", err)
		os.Exit(1)
	}

	// Handle OS signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Kafka consumer started...")

	// Start consuming messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: shutting down\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(-1) // -1 waits indefinitely
			if err == nil {
				fmt.Printf("Received message: %s\n", string(msg.Value))
			} else {
				fmt.Printf("Consumer error: %v\n", err)
			}
		}
	}
}
