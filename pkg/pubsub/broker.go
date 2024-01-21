package pubsub

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	pb "github.com/JyotinderSingh/pubsub/pkg/grpcapi"
	"google.golang.org/grpc"
)

type subscriberStream pb.PubSubService_SubscribeServer

// Uniquely identifies a subscriber stream for a given topic.
type streamKey struct {
	topic        string
	subscriberId uint32
}

type Broker struct {
	pb.UnimplementedPubSubServiceServer
	port                         string
	listener                     net.Listener
	grpcServer                   *grpc.Server
	subscribers                  map[string]map[uint32]subscriberStream // Map of topic to subscriber stream.
	topicSubscriberStreamMutexes map[streamKey]*sync.Mutex              // Mutex for each subscriber stream
	mu                           sync.RWMutex
	logger                       *zap.Logger
	ctx                          context.Context
	cancel                       context.CancelFunc
}

func NewBroker(port string) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{
		port:                         port,
		subscribers:                  make(map[string]map[uint32]subscriberStream),
		topicSubscriberStreamMutexes: make(map[streamKey]*sync.Mutex),
		logger:                       zap.Must(zap.NewProduction()),
		ctx:                          ctx,
		cancel:                       cancel,
	}
}

func (b *Broker) Start() error {
	var err error
	if err = b.startGRPCServer(); err != nil {
		return err
	}

	return b.awaitShutdown()
}

func (b *Broker) startGRPCServer() error {
	var err error
	b.listener, err = net.Listen("tcp", b.port)
	if err != nil {
		return err
	}

	b.logger.Info("Broker started", zap.String("port", b.port))

	b.grpcServer = grpc.NewServer()
	pb.RegisterPubSubServiceServer(b.grpcServer, b)

	b.logger.Info("Broker ready to serve requests.")

	go func() {
		if err := b.grpcServer.Serve(b.listener); err != nil {
			b.logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	return nil
}

func (b *Broker) Stop() error {
	b.cancel()
	b.grpcServer.GracefulStop()
	if err := b.listener.Close(); err != nil {
		b.logger.Info("Failed to close listener", zap.Error(err))
	}
	return nil
}

func (b *Broker) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return b.Stop()
}

func (b *Broker) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	b.mu.Lock()

	key := streamKey{topic: in.GetTopic(), subscriberId: in.GetSubscriberId()}

	if _, ok := b.subscribers[key.topic]; !ok {
		b.subscribers[key.topic] = make(map[uint32]subscriberStream)
	}

	b.subscribers[key.topic][key.subscriberId] = stream

	b.topicSubscriberStreamMutexes[key] = &sync.Mutex{}

	b.logger.Info("New subscriber", zap.String("topic", key.topic), zap.Uint32("id", key.subscriberId))

	b.mu.Unlock()

	for {
		select {
		// Wait for the client to close the stream
		case <-stream.Context().Done():
			return nil
			// Wait for the broker to shutdown
		case <-b.ctx.Done():
			return nil
		}
	}
}

func (b *Broker) Unsubscribe(ctx context.Context, in *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := streamKey{topic: in.GetTopic(), subscriberId: in.GetSubscriberId()}

	if _, ok := b.subscribers[key.topic]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	if _, ok := b.subscribers[key.topic][key.subscriberId]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	delete(b.topicSubscriberStreamMutexes, key)

	b.logger.Info("Subscriber unsubscribed", zap.String("topic", key.topic), zap.Uint32("id", key.subscriberId))
	return &pb.UnsubscribeResponse{Success: true}, nil
}

func (b *Broker) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	b.mu.RLock()

	brokenSubscribers := make([]streamKey, 0)
	for subscriberId, stream := range b.subscribers[in.GetTopic()] {
		// Acquire a lock on this subscriber's stream for this topic.
		key := streamKey{topic: in.GetTopic(), subscriberId: subscriberId}
		b.topicSubscriberStreamMutexes[key].Lock()

		err := stream.Send(&pb.Message{Topic: in.GetTopic(), Message: in.GetMessage()})

		// Release the lock on this subscriber's stream for this topic.
		b.topicSubscriberStreamMutexes[key].Unlock()
		if err != nil {
			b.logger.Error("Failed to send message to subscriber", zap.Uint32("id", subscriberId), zap.Error(err))
			// Add to broken subscribers list so that we can remove it later.
			brokenSubscribers = append(brokenSubscribers, streamKey{topic: in.GetTopic(), subscriberId: subscriberId})
		}
	}
	b.mu.RUnlock()

	// If we have broken subscribers, we should remove them from the list
	b.removeBrokenSubscribers(brokenSubscribers)

	if len(brokenSubscribers) > 0 {
		return &pb.PublishResponse{Success: false}, fmt.Errorf("failed to send to some subscribers")
	}

	return &pb.PublishResponse{Success: true}, nil
}

// Removes broken subscribers from all the topics.
func (b *Broker) removeBrokenSubscribers(keys []streamKey) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Iterate through all topics and remove broken subscribers
	for _, key := range keys {
		delete(b.subscribers[key.topic], key.subscriberId)
		delete(b.topicSubscriberStreamMutexes, key)
	}
}
