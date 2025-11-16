package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	broker := "localhost:9092"
	groupID := "order-consumers"
	topic := "orders"

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	// We will close consumer explicitly on shutdown.

	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	// Channels for messages, errors and OS signals
	msgCh := make(chan *kafka.Message, 16)
	errCh := make(chan error, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Reader goroutine: blocks on ReadMessage and forwards results
	go func() {
		for {
			m, err := c.ReadMessage(-1)
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- m
		}
	}()

	run := true
	log.Println("consumer started, waiting for messages... (Ctrl+C to stop)")
	for run {
		select {
		case sig := <-sigCh:
			log.Printf("signal received (%v), shutting down consumer...", sig)
			// Closing consumer will cause ReadMessage to return with error and goroutine to exit
			if err := c.Close(); err != nil {
				log.Printf("error closing consumer: %v", err)
			}
			run = false

		case err := <-errCh:
			// ReadMessage returned an error (could be due to Close)
			log.Printf("consumer read error: %v", err)
			// if consumer was closed intentionally, exit loop
			run = false

		case msg := <-msgCh:
			// Process message
			var val map[string]interface{}
			if err := json.Unmarshal(msg.Value, &val); err == nil {
				log.Printf("Received order: %v (partition=%d, offset=%d)", val, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			} else {
				log.Printf("Received message but failed to unmarshal: %s", string(msg.Value))
			}
			// Try to commit the message synchronously (optional)
			if _, err := c.CommitMessage(msg); err != nil {
				log.Printf("failed to commit message: %v", err)
			}
		}
	}

	log.Println("consumer shutdown complete")
}
