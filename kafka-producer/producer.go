package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	//delivery reports
	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*kafka.Message); ok {
				if msg.TopicPartition.Error != nil {
					fmt.Printf("delivery failed: %v\n", msg.TopicPartition.Error)
				} else {
					fmt.Printf("delivered to %v\n", msg.TopicPartition)
				}
			} else {
				log.Println("Waiting for the message...")
			}
		}
	}()

	topic := "orders"
	key := []byte("order-123")
	value := []byte(`{"id":"order-123","status":"created"}`)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	p.Flush(15000)
}
