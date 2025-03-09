package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Stock struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Volume int     `json:"volume"`
}

// Kafka broker and topic configuration
const (
	brokerAddress = "localhost:9092"
	groupID       = "st1-consumer-group"
	errorChance   = 10 // 10% failure rate
	maxRetries    = 3  // Max retry attempts before sending to DLQ
	dlqSuffix     = "-dlq"
)

// getDLQTopicName returns the dead letter queue topic name for a given topic
func getDLQTopicName(topic string) string {
	return topic + dlqSuffix
}

func main() {
	rand.Seed(time.Now().UnixNano()) // Initialize randomness

	// Topics to consume from
	topics := []string{"stock-topic", "invoice-topic", "metrics-topic"}

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Create DLQ writers map - one for each topic
	dlqWriters := make(map[string]*kafka.Writer)

	// Create a Kafka writer for each topic's DLQ
	for _, topic := range topics {
		dlqTopic := getDLQTopicName(topic)
		dlqWriters[topic] = &kafka.Writer{
			Addr:  kafka.TCP(brokerAddress),
			Topic: dlqTopic,
		}
		fmt.Printf("Created DLQ writer for topic %s -> %s\n", topic, dlqTopic)
	}

	// Clean up all writers when done
	defer func() {
		for _, writer := range dlqWriters {
			writer.Close()
		}
	}()

	// Use WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Create a consumer for each topic
	for _, topic := range topics {
		wg.Add(1)
		// Pass the appropriate DLQ writer for this topic
		go consumeTopic(ctx, topic, dlqWriters[topic], &wg)
	}

	fmt.Printf("Kafka consumer is running and listening on topics: %s\n",
		strings.Join(topics, ", "))

	// Wait for termination signal
	<-signals
	fmt.Println("\nShutdown signal received, closing consumers...")

	// Trigger cancellation for all consumers
	cancel()

	// Wait for all consumers to finish
	wg.Wait()
	fmt.Println("All consumers have shut down.")
}

// consumeTopic handles messages from a single topic
func consumeTopic(ctx context.Context, topic string, dlqWriter *kafka.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create Kafka reader (consumer) for this topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.LastOffset, // Start from the latest message
		MinBytes:    10e3,             // 10KB min per fetch
		MaxBytes:    10e6,             // 10MB max per fetch
	})
	defer reader.Close()

	dlqTopic := getDLQTopicName(topic)
	fmt.Printf("Started consumer for topic: %s (DLQ: %s)\n", topic, dlqTopic)

	// Consume messages from this topic
	for {
		// Check if context is cancelled before attempting to read
		select {
		case <-ctx.Done():
			fmt.Printf("Shutting down consumer for topic: %s\n", topic)
			return
		default:
			// Continue processing
		}

		// Read a message from the topic
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading from topic %s: %v", topic, err)
			continue
		}

		// Create a context with timeout for each read
		// readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		// msg, err := reader.ReadMessage(readCtx)
		// readCancel()

		// // If context cancelled while reading, exit
		// if ctx.Err() != nil {
		//     return
		// }

		// // Handle other errors
		// if err != nil {
		//     // Check if it's a timeout (expected) or real error
		//     if err != context.DeadlineExceeded {
		//         log.Printf("Error reading from topic %s: %v", topic, err)
		//     }
		//     continue
		// }

		// Process the message
		fmt.Printf("\n---- Message from topic %s ----\n", topic)
		fmt.Printf("Received message: %s\n", string(msg.Value))

		// Process based on topic
		var processErr error
		switch topic {
		case "stock-topic":
			processErr = processStockMessage(msg.Value)
		default:
			processErr = processGenericMessage(msg.Value, topic)
		}

		// Handle processing errors
		if processErr != nil {
			fmt.Printf("Processing error on topic %s: %v\n", topic, processErr)

			// Try processing with retries
			if err := retryProcessMessage(msg.Value, topic, maxRetries); err != nil {
				fmt.Printf("Max retries reached for topic %s, sending to DLQ: %s\n", topic, dlqTopic)

				// Send to topic-specific dead-letter queue
				err := dlqWriter.WriteMessages(ctx, kafka.Message{
					Key:   msg.Key,
					Value: msg.Value,
					//Value: msg.Value,
					Time: time.Now(),
					Headers: []kafka.Header{
						{
							Key:   "original-topic",
							Value: []byte(topic),
						},
						{
							Key:   "error-message",
							Value: []byte(err.Error()),
						},
						{
							Key:   "failed-at",
							Value: []byte(time.Now().Format(time.RFC3339)),
						},
						{
							Key:   "retry-count",
							Value: []byte(fmt.Sprintf("%d", maxRetries)),
						},
					},
				})
				if err != nil {
					log.Printf("Failed to write to DLQ %s: %v", dlqTopic, err)
				} else {
					fmt.Printf("Message sent to DLQ %s successfully\n", dlqTopic)
				}
			}
		}
	}
}

// Process stock topic messages
func processStockMessage(msgValue []byte) error {
	var stock Stock
	if err := json.Unmarshal(msgValue, &stock); err != nil {
		return fmt.Errorf("failed to parse stock data: %w", err)
	}

	// if rand.Intn(100) < errorChance { // 50% chance of failure
	// 	return fmt.Errorf("random stock processing error")
	// }

	// Print Stock details
	fmt.Printf("Stock Symbol: %s\n", stock.Symbol)
	fmt.Printf("Stock Price: %.2f\n", stock.Price)
	fmt.Printf("Stock Volume: %d\n", stock.Volume)
	return nil
}

// Process messages from other topics
func processGenericMessage(msgValue []byte, topic string) error {
	if rand.Intn(100) < errorChance/2 { // 25% chance of failure
		return fmt.Errorf("random processing error on topic %s", topic)
	}

	fmt.Printf("Successfully processed message from %s: %s\n",
		topic, string(msgValue))
	return nil
}

// Retry message processing with exponential backoff
func retryProcessMessage(msg []byte, topic string, retries int) error {
	var err error
	for attempt := 1; attempt <= retries; attempt++ {
		fmt.Printf("Retry attempt %d/%d for topic %s\n", attempt, retries, topic)

		// Process based on topic
		switch topic {
		case "stock-topic":
			err = processStockMessage(msg)
		default:
			err = processGenericMessage(msg, topic)
		}

		if err == nil {
			return nil
		}

		fmt.Printf("Attempt %d/%d failed for topic %s: %v\n",
			attempt, retries, topic, err)

		// Exponential backoff (e.g., 1s, 2s, 4s)
		backoff := time.Duration(1<<attempt) * time.Second
		fmt.Printf("Waiting %v before next retry...\n", backoff)
		time.Sleep(backoff)
	}
	return err
}
