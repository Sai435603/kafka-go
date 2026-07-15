package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func newProducer() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})

	if err != nil {
		log.Println(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed due to the err: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %v (Offset: %v)\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Offset)
				}
			default:
				fmt.Println("Waiting for the events...")
			}
		}
	}()
	topic := "orders"
	for i := range 10 { // for i:=0; i<10; i++ {}
		val := fmt.Sprintf("orders payload #%d", i)
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(val),
		}, nil)

		if err != nil {
			fmt.Printf("Producer error %v\n", err)
		}
	}
	p.Flush(15 * 1000)
}
