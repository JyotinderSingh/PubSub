package pubsub

import (
	"context"

	pb "github.com/JyotinderSingh/pubsub/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Publisher struct {
	brokerAddress string
	conn          *grpc.ClientConn
	client        pb.PubSubServiceClient
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewPublisher creates a new publisher which connects to the broker at the given address.
func NewPublisher(brokerAddress string) (*Publisher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	publisher := &Publisher{
		brokerAddress: brokerAddress,
		ctx:           ctx,
		cancel:        cancel,
	}

	var err error
	publisher.conn, err = grpc.Dial(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	publisher.client = pb.NewPubSubServiceClient(publisher.conn)

	return publisher, nil
}

// Publish publishes a message to the given topic.
func (p *Publisher) Publish(topic string, message []byte) error {
	_, err := p.client.Publish(p.ctx, &pb.PublishRequest{
		Topic:   topic,
		Message: message,
	})
	return err
}

// Close closes the publisher connection.
func (p *Publisher) Close() error {
	p.cancel()
	return p.conn.Close()
}
