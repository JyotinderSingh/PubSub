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

type subscriber struct {
	id     uint32
	stream pb.PubSubService_SubscribeServer
}

type topicAndSubscriber struct {
	topic      string
	subscriber uint32
}

type Broker struct {
	pb.UnimplementedPubSubServiceServer
	port                         string
	listener                     net.Listener
	grpcServer                   *grpc.Server
	subscribers                  map[string]map[uint32]subscriber  // Map of topic to subscriber stream.
	topicSubscriberStreamMutexes map[string]map[uint32]*sync.Mutex // Mutex for each subscriber stream
	mu                           sync.RWMutex
	logger                       *zap.Logger
}

func NewBroker(port string) *Broker {
	return &Broker{
		port:                         port,
		subscribers:                  make(map[string]map[uint32]subscriber),
		topicSubscriberStreamMutexes: make(map[string]map[uint32]*sync.Mutex),
		logger:                       zap.Must(zap.NewProduction()),
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

	if _, ok := b.subscribers[in.GetTopic()]; !ok {
		b.subscribers[in.GetTopic()] = make(map[uint32]subscriber)
	}

	subscriberInfo := subscriber{id: in.GetSubscriberId(), stream: stream}
	b.subscribers[in.GetTopic()][in.GetSubscriberId()] = subscriberInfo

	if _, ok := b.topicSubscriberStreamMutexes[in.GetTopic()]; !ok {
		b.topicSubscriberStreamMutexes[in.GetTopic()] = make(map[uint32]*sync.Mutex)
	}

	if _, ok := b.topicSubscriberStreamMutexes[in.GetTopic()][in.GetSubscriberId()]; !ok {
		b.topicSubscriberStreamMutexes[in.GetTopic()][in.GetSubscriberId()] = &sync.Mutex{}
	}

	b.mu.Unlock()

	b.logger.Info("New subscriber", zap.String("topic", in.GetTopic()), zap.Uint32("id", in.GetSubscriberId()))

	<-subscriberInfo.stream.Context().Done()
	// Wait for the client to close the stream
	return nil

}

func (b *Broker) Unsubscribe(ctx context.Context, in *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscribers[in.GetTopic()]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	if _, ok := b.subscribers[in.GetTopic()][in.GetSubscriberId()]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	delete(b.subscribers[in.GetTopic()], in.GetSubscriberId())

	if _, ok := b.topicSubscriberStreamMutexes[in.GetTopic()]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	if _, ok := b.topicSubscriberStreamMutexes[in.GetTopic()][in.GetSubscriberId()]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	delete(b.topicSubscriberStreamMutexes[in.GetTopic()], in.GetSubscriberId())

	b.logger.Info("Subscriber unsubscribed", zap.String("topic", in.GetTopic()), zap.Uint32("id", in.GetSubscriberId()))
	return &pb.UnsubscribeResponse{Success: true}, nil
}

func (b *Broker) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	b.mu.RLock()

	// brokenSubscribers stores topic and subsriber id of the subscribers that are broken.
	// We will remove them from the list later.
	brokenSubscribers := make([]topicAndSubscriber, 0)
	for _, sub := range b.subscribers[in.GetTopic()] {
		// Get the mutex for this subscriber
		b.topicSubscriberStreamMutexes[in.GetTopic()][sub.id].Lock()
		err := sub.stream.Send(&pb.Message{Topic: in.GetTopic(), Message: in.GetMessage()})
		if err != nil {
			b.logger.Error("Failed to send message to subscriber", zap.Uint32("id", sub.id), zap.Error(err))
			// Add to broken subscribers list so that we can remove it later.
			brokenSubscribers = append(brokenSubscribers, topicAndSubscriber{topic: in.GetTopic(), subscriber: sub.id})
		}
		b.topicSubscriberStreamMutexes[in.GetTopic()][sub.id].Unlock()
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
func (b *Broker) removeBrokenSubscribers(topicsAndSubscribers []topicAndSubscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Iterate through all topics and remove broken subscribers
	for _, topicAndSubscriber := range topicsAndSubscribers {
		delete(b.subscribers[topicAndSubscriber.topic], topicAndSubscriber.subscriber)
		delete(b.topicSubscriberStreamMutexes[topicAndSubscriber.topic], topicAndSubscriber.subscriber)
	}
}
