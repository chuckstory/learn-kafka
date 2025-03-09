package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type Stock struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Volume int     `json:"volume"`
}

type Invoice struct {
	InvoiceNumber string  `json:"invoice_number"`
	Amount        float64 `json:"amount"`
	Customer      string  `json:"customer"`
}

// KafkaProducer wraps a Sarama SyncProducer
type KafkaProducer struct {
	producer sarama.SyncProducer
}

// NewKafkaProducer creates and returns a new KafkaProducer
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer: producer}, nil
}

// Close closes the underlying Sarama producer
func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}

// SendStockMessage sends a stock object to Kafka
func (kp *KafkaProducer) SendStockMessage(stock Stock) (int32, int64, error) {
	jsonData, err := json.Marshal(stock)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal stock: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: "stock-topic",
		Key:   sarama.StringEncoder(stock.Symbol),
		Value: sarama.StringEncoder(jsonData),
	}

	return kp.producer.SendMessage(msg)
}

// SendInvoiceMessage sends an invoice object to Kafka
func (kp *KafkaProducer) SendInvoiceMessage(invoice Invoice) (int32, int64, error) {
	jsonData, err := json.Marshal(invoice)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal invoice: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: "invoice-topic",
		Key:   sarama.StringEncoder(invoice.InvoiceNumber),
		Value: sarama.StringEncoder(jsonData),
	}

	return kp.producer.SendMessage(msg)
}

// SendTextMessage sends a text message to Kafka with an optional key
func (kp *KafkaProducer) SendTextMessage(key, value string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: "news-topic",
		Value: sarama.StringEncoder(value),
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	return kp.producer.SendMessage(msg)
}

// InputHandler manages user input and message formatting
type InputHandler struct {
	producer *KafkaProducer
	scanner  *bufio.Scanner
	doneCh   chan struct{}
}

// NewInputHandler creates a new InputHandler
func NewInputHandler(producer *KafkaProducer) *InputHandler {
	return &InputHandler{
		producer: producer,
		scanner:  bufio.NewScanner(os.Stdin),
		doneCh:   make(chan struct{}),
	}
}

// Start begins the input handling process
func (ih *InputHandler) Start() chan struct{} {
	go ih.processInput()
	return ih.doneCh
}

// processInput handles user input and sends messages to Kafka
func (ih *InputHandler) processInput() {
	fmt.Println("Enter stock data in format 'symbol:price:volume' (type 'exit' to quit)")
	fmt.Println("Example: AAPL:150.50:1000")
	fmt.Println("Or enter messages in format 'key:value' (regular text)")
	fmt.Println("For regular text, if no key is provided, a nil key will be used")

	for {
		fmt.Print("> ")
		if !ih.scanner.Scan() {
			break
		}

		text := ih.scanner.Text()
		if text == "exit" {
			break
		}

		parts := strings.Split(text, ":")

		// Check if input matches stock format (symbol:price:volume)
		if len(parts) == 3 {
			ih.handleStockInput(parts)
		} else {
			ih.handleTextInput(text)
		}
	}

	close(ih.doneCh)
}

// handleStockInput processes input in the stock format
func (ih *InputHandler) handleStockInput(parts []string) {
	fmt.Println("Stock data detected")

	symbol := parts[0]
	price, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		fmt.Println("Invalid price format. Use a number.")
		return
	}

	volume, err := strconv.Atoi(parts[2])
	if err != nil {
		fmt.Println("Invalid volume format. Use an integer.")
		return
	}

	// Create stock object
	stock := Stock{
		Symbol: symbol,
		Price:  price,
		Volume: volume,
	}

	fmt.Printf("Sending stock: %s at $%.2f with volume %d\n", symbol, price, volume)

	partition, offset, err := ih.producer.SendStockMessage(stock)
	if err != nil {
		log.Printf("Failed to send stock message: %v", err)
		return
	}

	log.Printf("Stock sent to partition %d at offset %d\n", partition, offset)
}

// handleTextInput processes regular text input
func (ih *InputHandler) handleTextInput(text string) {
	var key, value string
	parts := strings.SplitN(text, ":", 2)

	if len(parts) == 2 {
		key = parts[0]
		value = parts[1]
		fmt.Printf("Using key: %s, value: %s\n", key, value)
	} else {
		value = text
		fmt.Printf("No key provided. Using value: %s\n", value)
	}

	partition, offset, err := ih.producer.SendTextMessage(key, value)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}

	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}

func main() {
	// Create Kafka producer
	kafkaProducer, err := NewKafkaProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatalln("Failed to start Kafka producer:", err)
	}
	defer kafkaProducer.Close()

	// Set up signal handling
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	done := make(chan struct{})

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(10)

		for i := 0; i < 2; i++ {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					stock := Stock{
						Symbol: "AAPLm" + strconv.Itoa(i),
						Price:  150.50 * float64(i),
						Volume: 1000,
					}
					partition, offset, err := kafkaProducer.SendStockMessage(stock)
					if err != nil {
						log.Printf("Failed to send stock message: %v", err)
						return
					}

					log.Printf("Stock sent to partition %d at offset %d\n", partition, offset)

					invoice := Invoice{
						InvoiceNumber: "INV" + strconv.Itoa(i),
						Amount:        150.50 * float64(i),
						Customer:      "Customer" + strconv.Itoa(i),
					}

					partitionInvoice, offsetInvoice, errInvoice := kafkaProducer.SendInvoiceMessage(invoice)
					if errInvoice != nil {
						log.Printf("Failed to send invoice message: %v", errInvoice)
						return
					}

					log.Printf("Invoice sent to partition %d at offset %d\n", partitionInvoice, offsetInvoice)

					// Sleep for a random duration
					time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
				}
			}(i)
		}

		wg.Wait()
		fmt.Println("All stock messages sent")

		close(done)
	}()

	// Create and start input handler
	// inputHandler := NewInputHandler(kafkaProducer)
	// doneCh := inputHandler.Start()

	// Wait for termination
	select {
	case <-signals:
		fmt.Println("\nReceived interrupt signal. Shutting down...")
	// case <-doneCh:
	// 	fmt.Println("Exiting program...")
	case <-done:
		fmt.Println("Exiting program...")
	}

	fmt.Println("Producer terminated")
}
