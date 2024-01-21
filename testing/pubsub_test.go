package testing

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/JyotinderSingh/pubsub/pkg/pubsub"
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
