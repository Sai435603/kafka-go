package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ConsumerWithDQL() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               "localhost:9092",
		"group.id":                        "order-service-grp",
		"enable.auto.commit":              false,
		"auto.reset.offset":               "earliest",
		"go.application.rebalance.enable": true,
	})

	if err != nil {
		log.Println(err)
	}
	defer c.Close()

	//producer for the dead letter queue
	dlqProducer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	defer dlqProducer.Close()
	dlqTopic := "orders-topic-dlq"
	c.SubscribeTopics([]string{"orders"}, nil)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Starting the Consumer...")
	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(50) //polls every 50s lol will try 69 next time
			if ev == nil {
				continue
			}

			switch msg := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Processing message on %s: %s\n", msg.TopicPartition, string(msg.Value))
				err := ProcessOrders(msg.Value)
				if err != nil {
					//triggering DLQ
					fmt.Printf("Error processing, sending to DLQ: %v\n", err)
					dlqProducer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
						Key:            msg.Key,
						Value:          msg.Value,
					}, nil)
				}
				_, err = c.CommitMessage(msg)
				if err != nil {
					fmt.Printf("Failed to commit offset: %v\n", err)
				} else {
					fmt.Printf("Successfully committed offset %v\n", msg.TopicPartition.Offset)
				}
			case kafka.AssignedPartitions:
				fmt.Printf("Rebalance: Assigned to new partitions: %v\n", msg.Partitions)
				c.Assign(msg.Partitions)
			case kafka.RevokedPartitions:
				fmt.Printf("Rebalance: Partitions revoked from this consumer: %v\n", msg.Partitions)
				c.Unassign()
			case kafka.Error:
				fmt.Printf("Kafka Error: %v\n", msg)

			}
		}
	}
}

func ProcessOrders(order []byte) error {
	data := string(order)
	if data == "orders payload #5" {
		return fmt.Errorf("corrupted payload")
	}
	return nil
}
