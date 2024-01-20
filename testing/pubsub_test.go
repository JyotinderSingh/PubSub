package testing

import (
	"testing"
	"time"

	"github.com/JyotinderSingh/pubsub/pkg/pubsub"
)

var broker *pubsub.Broker

// Setup function to start the broker
func TestMain(m *testing.M) {
	broker = pubsub.NewBroker(":50054")
	// Start the broker in a new goroutine and handle any errors
	go func() {
		if err := broker.Start(); err != nil {
			panic(err)
		}
	}()

	m.Run()
}

// Cleanup
func TestTeardown(t *testing.T) {
	if err := broker.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	publisher, err := pubsub.NewPublisher("localhost:50054")
	if err != nil {
		t.Fatal(err)
	}

	defer publisher.Close()

	consumer, err := pubsub.NewConsumer("localhost:50054")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	topic := "topic1"
	// Subscribe to a topic
	consumer.Subscribe(topic)
	time.Sleep(100 * time.Millisecond)

	messages := []string{"hello", "world", "!", "This", "is", "topic", "1"}
	for _, message := range messages {

		// Publish a message
		if err := publisher.Publish(topic, []byte(message)); err != nil {
			t.Fatal(err)
		}
	}

	// Consume the message
	for i := 0; i < len(messages); i++ {
		message := <-consumer.Messages
		if message.Topic != topic {
			t.Fatalf("Expected topic %s, got %s", topic, message.Topic)
		}

		if string(message.GetMessage()) != messages[i] {
			t.Fatalf("Expected message %s, got %s", messages[i], string(message.GetMessage()))
		}
	}

	// Publish and consume on another channel.
	topic = "topic2"
	consumer.Subscribe(topic)
	time.Sleep(100 * time.Millisecond)

	messages = []string{"hello", "world", "!", "This", "is", "topic", "2"}

	for _, message := range messages {
		if err := publisher.Publish(topic, []byte(message)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < len(messages); i++ {
		message := <-consumer.Messages

		if message.Topic != topic {
			t.Fatalf("Expected topic %s, got %s", topic, message.Topic)
		}

		if string(message.GetMessage()) != messages[i] {
			t.Fatalf("Expected message %s, got %s", messages[i], string(message.GetMessage()))
		}
	}
}
