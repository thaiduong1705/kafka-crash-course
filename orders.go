package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// Goroutine bắt delivery reports & errors
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered to %v\n", ev.TopicPartition)
				}
			case kafka.Error:
				fmt.Printf("Kafka error: %v\n", ev)
			default:
				// other events
			}
		}
	}()

	order := map[string]interface{}{
		"order_id": uuid.New().String(),
		"user":     "Thai",
		"item":     "Book",
		"quantity": 1,
	}
	orderBytes, _ := json.Marshal(order)

	topic := "orders"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          orderBytes,
	}, nil)
	if err != nil {
		fmt.Println("Produce error:", err)
	}

	// Đợi events xử lý xong
	producer.Flush(15000)
	// small wait để goroutine in ra (nếu cần)
	time.Sleep(500 * time.Millisecond)
}
