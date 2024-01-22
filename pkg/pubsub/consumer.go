package pubsub

import (
	"context"
	"fmt"
	"io"
	"sync"

	pb "github.com/JyotinderSingh/pubsub/pkg/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	ID            uint32
	brokerAddress string
	conn          *grpc.ClientConn
	client        pb.PubSubServiceClient
	Messages      chan *pb.Message // Channel to receive messages from the broker.
	subscriptions sync.Map         // map[string]context.CancelFunc
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewConsumer creates a new consumer which connects to the broker at the given address.
func NewConsumer(brokerAddress string) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		ID:            uuid.New().ID(),
		brokerAddress: brokerAddress,
		subscriptions: sync.Map{},
		Messages:      make(chan *pb.Message, 500),
		ctx:           ctx,
		cancel:        cancel,
	}

	var err error
	consumer.conn, err = grpc.Dial(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	consumer.client = pb.NewPubSubServiceClient(consumer.conn)

	return consumer, nil
}

// Subscribe subscribes to the given topic.
func (c *Consumer) Subscribe(topic string) error {
	// Check if already subscribed
	_, ok := c.subscriptions.Load(topic)
	if ok {
		return nil
	}

	// Context to manage the stream.
	streamCtx, streamCancel := context.WithCancel(c.ctx)

	stream, err := c.client.Subscribe(streamCtx, &pb.SubscribeRequest{SubscriberId: c.ID, Topic: topic})

	if err != nil {
		streamCancel()
		return err
	}

	c.subscriptions.Store(topic, streamCancel)

	// Start a goroutine to receive messages from the broker.
	go c.receive(stream, streamCtx)

	return nil
}

// Unsubscribe unsubscribes from the given topic.
func (c *Consumer) Unsubscribe(topic string) error {
	// Check if already unsubscribed.
	cancel, ok := c.subscriptions.Load(topic)
	if !ok {
		return fmt.Errorf("not subscribed to topic %s", topic)
	}

	// Cancel the context to stop the receive goroutine.
	cancel.(context.CancelFunc)()
	// Remove the topic from the subscriptions map.
	c.subscriptions.Delete(topic)

	// Send an unsubscribe request to the broker.
	_, err := c.client.Unsubscribe(c.ctx, &pb.UnsubscribeRequest{Topic: topic, SubscriberId: c.ID})

	return err
}

// receive receives messages from the broker on the given stream and pushes them
// to the Messages channel.
func (c *Consumer) receive(stream pb.PubSubService_SubscribeClient, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			stream.CloseSend()
			return
		default:
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				return
			}

			// Push the message to the channel.
			c.Messages <- msg
		}
	}
}

func (c *Consumer) Close() error {
	c.cancel()
	// Iterate through all the subscriptions and unsubscribe from them.
	c.subscriptions.Range(func(key, value interface{}) bool {
		c.Unsubscribe(key.(string))
		return true
	})

	return c.conn.Close()
}
