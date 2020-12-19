package main

import (
	"fmt"
	"irc-kafka-consumer/kafka"
	"sync"
)

func main() {
	con := kafka.Connect(kafka.KafkaConnectionConfig{
		Brokers: []string{"localhost:9092"},
		Topics:  []string{"topic"},
		Group:   "group",
	})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for msg := range con.Output() {
			msg.Acknowledge()
			fmt.Println(msg.Message)
		}
	}()
	wg.Wait()
}
