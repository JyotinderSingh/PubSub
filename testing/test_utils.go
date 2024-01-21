package testing

import (
	"testing"

	"github.com/JyotinderSingh/pubsub/pkg/pubsub"
	"github.com/stretchr/testify/assert"

	pb "github.com/JyotinderSingh/pubsub/pkg/grpcapi"
)

func publishMessages(t *testing.T, publisher *pubsub.Publisher, topic string, messages []string) {
	go func() {
		for _, message := range messages {
			err := publisher.Publish(topic, []byte(message))
			assert.NoError(t, err, "Failed to publish message")
		}
	}()
}

func receiveMessages(t *testing.T, consumer *pubsub.Consumer, topic string, messages []string) {
	go func() {
		messageMap := make(map[string]int)
		for i := 0; i < len(messages)*2; i++ {
			message := <-consumer.Messages
			// Put the message into a map with the topic + message as the key and the value as the number of occurrences.
			messageMap[message.Topic+string(message.GetMessage())]++
		}

		// Check that each message was received twice.
		for _, message := range messages {
			assert.Equal(t, 2, messageMap[topic+message], "Expected message %s to be received twice", message)
		}
	}()
}

func setupPublisher(t *testing.T, address string) *pubsub.Publisher {
	publisher, err := pubsub.NewPublisher(address)
	assert.NoError(t, err, "Failed to create publisher")
	return publisher
}

func setupConsumer(t *testing.T, address string) *pubsub.Consumer {
	consumer, err := pubsub.NewConsumer(address)
	assert.NoError(t, err, "Failed to create consumer")
	return consumer
}

func assertMessage(t *testing.T, expectedTopic, expectedMessage string, message *pb.Message) {
	assert.Equal(t, expectedTopic, message.Topic, "Expected topic %s, got %s", expectedTopic, message.Topic)

	assert.Equal(t, expectedMessage, string(message.GetMessage()), "Expected message %s, got %s", expectedMessage, string(message.GetMessage()))
}
