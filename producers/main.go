package main

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func main() {
	topic := "orders"
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "order-producer",
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
					fmt.Printf("Delivered to partition %v, offset %v, topic %v\n", ev.TopicPartition.Partition, ev.TopicPartition.Offset, *ev.TopicPartition.Topic)
					// convert ev.Value back to string
					var deliveredOrder map[string]interface{}
					if err := json.Unmarshal(ev.Value, &deliveredOrder); err == nil {
						fmt.Printf("Delivered order: %v\n", deliveredOrder)
					}

				}
			case kafka.Error:
				fmt.Printf("Kafka error: %v\n", ev)
			default:
				// other events
			}
		}
	}()

	for i := 0; i < 1; i++ {
		order := map[string]interface{}{
			"order_id": uuid.New().String(),
			"user":     fmt.Sprintf("user-%d", i),
			"item":     "Book",
			"quantity": i + 1,
		}
		orderBytes, _ := json.Marshal(order)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: orderBytes,
		}
		if err := producer.Produce(msg, nil); err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
			return
		}
	}
	fmt.Println("Message produced successfully")

	producer.Flush(5000)
}
