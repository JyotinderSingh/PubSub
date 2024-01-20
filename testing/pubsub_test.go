package testing

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/JyotinderSingh/pubsub/pkg/pubsub"
	"github.com/stretchr/testify/assert"

	pb "github.com/JyotinderSingh/pubsub/pkg/grpcapi"
)

var broker *pubsub.Broker

func setup() {
	broker = pubsub.NewBroker(":50054")

	go func() {
		if err := broker.Start(); err != nil {
			log.Fatalf("failed to start broker: %v", err)
		}
	}()
}

func teardown() {
	if err := broker.Stop(); err != nil {
		log.Fatalf("Failed to stop broker: %v", err)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// Single publisher multiple subscribers.
func TestPublishAndSubscribe(t *testing.T) {
	setup()
	defer teardown()

	testCases := []struct {
		topic    string
		messages []string
	}{
		{"topic1", []string{"hello", "world", "!", "This", "is", "topic", "1"}},
		{"topic2", []string{"hello", "world", "!", "This", "is", "topic", "2"}},
	}

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			publisher := setupPublisher(t, "localhost:50054")
			consumer1 := setupConsumer(t, "localhost:50054")
			consumer2 := setupConsumer(t, "localhost:50054")

			defer publisher.Close()
			defer consumer1.Close()
			defer consumer2.Close()

			consumer1.Subscribe(tc.topic)
			consumer2.Subscribe(tc.topic)

			time.Sleep(100 * time.Millisecond)

			for _, message := range tc.messages {
				if err := publisher.Publish(tc.topic, []byte(message)); err != nil {
					t.Fatal(err)
				}
			}

			for i := 0; i < len(tc.messages); i++ {
				message := <-consumer1.Messages
				assertMessage(t, tc.topic, tc.messages[i], message)

				message = <-consumer2.Messages
				assertMessage(t, tc.topic, tc.messages[i], message)
			}
		})
	}
}

// Multiple publishers multiple subscribers.
func TestMultiplePublishersAndSubscribers(t *testing.T) {
	setup()
	defer teardown()

	testCases := []struct {
		topic    string
		messages []string
	}{
		{"topic1", []string{"hello", "world", "!", "This", "is", "topic", "1"}},
		{"topic2", []string{"hello", "world", "!", "This", "is", "topic", "2"}},
		{"topic3", []string{"hello", "world", "!", "This", "is", "topic", "3"}},
	}

	publisher1 := setupPublisher(t, "localhost:50054")
	defer publisher1.Close()

	publisher2 := setupPublisher(t, "localhost:50054")
	defer publisher2.Close()

	consumer1 := setupConsumer(t, "localhost:50054")
	defer consumer1.Close()

	consumer2 := setupConsumer(t, "localhost:50054")
	defer consumer2.Close()

	for _, tc := range testCases {
		t.Run(tc.topic, func(t *testing.T) {
			consumer1.Subscribe(tc.topic)
			consumer2.Subscribe(tc.topic)

			time.Sleep(100 * time.Millisecond)

			// Publish messages from both publishers to the same topic.
			publishMessages(t, publisher1, tc.topic, tc.messages)
			publishMessages(t, publisher2, tc.topic, tc.messages)

			// Receive messages from both consumers to the same topic.
			receiveMessages(t, consumer1, tc.topic, tc.messages)
			receiveMessages(t, consumer2, tc.topic, tc.messages)
		})
		time.Sleep(100 * time.Millisecond)
	}
}

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
