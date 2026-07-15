package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func consumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "order-service",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if err := c.SubscribeTopics([]string{"orders"}, nil); err != nil {
		log.Fatal(err)
	}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run {
		select {
		case <-sigchan:
			run = false
		default:
			ev := c.Poll(100) //even value
			if ev == nil {
				continue
			}

			switch msg := ev.(type) {
			case *kafka.Message:
				fmt.Printf("got message from kafka of topic : %s: %s\n", msg.TopicPartition, string(msg.Value))
				_, err := c.StoreMessage(msg)
				if err != nil {
					fmt.Printf("store offset failed: %v\n", err)
				}
			case kafka.Error:
				fmt.Printf("Kafka error %v \n", msg)
			}
		}
	}
}
