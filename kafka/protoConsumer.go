package kafka

import (
	"context"
	"fmt"
	"os"

	pb "irc-kafka-consumer/proto"

	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"
)

type (
	MessageConsumer interface {
		Output() <-chan ChatMessage
	}

	Consumer struct {
		ready  chan bool
		output chan ChatMessage
	}

	ChatMessage struct {
		Acknowledge func()
		Message     pb.ChatMessage
	}

	KafkaConnectionConfig struct {
		Brokers []string
		Topics  []string
		Group   string
	}
)

// TODO: Context
// TODO: Errors
func Connect(conConfig KafkaConnectionConfig) MessageConsumer {
	config := sarama.NewConfig()

	// Start at the first available offset
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer := Consumer{
		ready:  make(chan bool),
		output: make(chan ChatMessage),
	}

	client, err := sarama.NewConsumerGroup(conConfig.Brokers, conConfig.Group, config)
	ctx := context.Background()

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			// TODO: Pausing - buffer output channel & wait until it's got space?
			if err := client.Consume(ctx, conConfig.Topics, &consumer); err != nil {
				panic(err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready

	return &consumer
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var chatMessage pb.ChatMessage
		if err := proto.Unmarshal(message.Value, &chatMessage); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal pb due to %v", err)
		}
		consumer.output <- ChatMessage{
			Acknowledge: func() {
				session.MarkMessage(message, "")
			},
			Message: chatMessage,
		}
	}
	return nil
}

func (consumer *Consumer) Output() <-chan ChatMessage {
	return consumer.output
}
