package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	logger *slog.Logger
)

const (
	brokerAddress = "192.168.10.214:9092"
	topic         = "test_topic"
	groupID       = "consumer_group_1"
)

var (
	consumers  = 5
	partitions = 3
	messages   = 150
	logLevel   = slog.LevelInfo
	cleanExit  = true
)

func init() {
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	slog.SetDefault(logger)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		<-sigChan

		logger.Info("termination signal received, waiting for consumers...")
		cancel()
	}()

	newTopic(partitions)
	go newProducer(ctx)

	wg := sync.WaitGroup{}
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			newConsumer(ctx, consumerID, &wg)
		}(i)
	}

	<-ctx.Done()
	wg.Wait()

	if cleanExit {
		deleteTopic()
	}

	logger.Debug("all consumers stopped, exiting...")
}

func newProducer(ctx context.Context) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Version = sarama.V2_7_0_0
	config.Net.MaxOpenRequests = 1

	rand.Seed(time.Now().UnixNano()) // Seed to ensure randomness

	logger.Info("creating a producer...")
	producer, err := sarama.NewSyncProducer([]string{brokerAddress}, config)
	if err != nil {
		logger.Error(fmt.Sprintf("creating a producer failed: %v", err))
	}
	defer func(producer sarama.SyncProducer) {
		logger.Info("closing the producer...")
		err := producer.Close()
		if err != nil {
			logger.Error(fmt.Sprintf("closing the producer failed: %v", err))
		}

	}(producer)

	for i := 0; i < messages; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		message := fmt.Sprintf("message %d", i)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			logger.Error(fmt.Sprintf("sending message failed: %v", err))

			time.Sleep(1 * time.Second)
			continue
		}

		logger.Info(fmt.Sprintf("producer sent message: %s", message), "partition", partition, "offset", offset)
		time.Sleep(200 * time.Millisecond)
	}
}

func newConsumer(ctx context.Context, consumerId int, wg *sync.WaitGroup) {
	defer wg.Done()

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	logger.Info("creating a consumer group...", "consumer-group", groupID, "consumer-id", consumerId)
	consumerGroup, err := sarama.NewConsumerGroup([]string{brokerAddress}, groupID, config)
	if err != nil {
		logger.Error(fmt.Sprintf("creating a consumer group failed: %v", err), "consumer-group", groupID, "consumer-id", consumerId)
	}
	defer func(consumerGroup sarama.ConsumerGroup, consumerId int) {
		logger.Info("closing the consumer group...", "consumer-group", groupID, "consumer-id", consumerId)
		err := consumerGroup.Close()
		if err != nil {
			logger.Error(fmt.Sprintf("closing the consumer group failed: %v", err), "consumer-group", groupID, "consumer-id", consumerId)
		}
	}(consumerGroup, consumerId)

	handler := &ConsumerGroupHandler{
		consumerId:      consumerId,
		consumerGroupID: groupID,
	}

	for {
		select {
		case <-ctx.Done():
			logger.Debug("context cancellation received, closing the consumer...", "consumer-group", groupID, "consumer-id", consumerId)
			return
		default:
			err := consumerGroup.Consume(ctx, []string{topic}, handler)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					logger.Error(err.Error(), "consumer-group", groupID, "consumer-id", consumerId)
					return
				}
				logger.Info(fmt.Sprintf("consumer failed: %v", err), "consumer-group", groupID, "consumer-id", consumerId)
			}
		}
	}
}

type ConsumerGroupHandler struct {
	consumerId      int
	consumerGroupID string
	//ready           chan struct{}
}

func (c *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	logger.Debug("consumer joined the group (setup)", "consumer-group", c.consumerGroupID, "consumer-id", c.consumerId)
	return nil
}

func (c *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	logger.Debug("consumer left the group (cleanup)", "consumer-group", c.consumerGroupID, "consumer-id", c.consumerId)
	return nil
}

func (c *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka will attempt to rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			logger.Debug("consumer session context cancelled...", "consumer-group", c.consumerGroupID, "consumer-id", c.consumerId)
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				logger.Error("consuming message failed: message channel was closed...", "consumer-group", c.consumerGroupID, "consumer-id", c.consumerId)
				return nil
			}
			session.MarkMessage(msg, "")
			logger.Info(fmt.Sprintf("consumer claimed message: %s", string(msg.Value)),
				"partition", msg.Partition, "offset", msg.Offset, "consumer-group", c.consumerGroupID, "consumer-id", c.consumerId)

			/// Only for demonstration purposes to simulate some delay between producer and consumers.
			// Remove in production code!
			time.Sleep(1 * time.Second)
		}
	}
}

func newTopic(partitions int) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_7_0_0

	logger.Debug("creating cluster admin...")
	admin, err := sarama.NewClusterAdmin([]string{brokerAddress}, config)
	if err != nil {
		logger.Error(fmt.Sprintf("creating a cluster admin failed: %v", err))
	}
	defer func(admin sarama.ClusterAdmin) {
		logger.Debug("closing cluster admin...")
		err := admin.Close()
		if err != nil {
			logger.Error(fmt.Sprintf("closing the cluster admin failed: %v", err))
		}

	}(admin)

	logger.Debug("creating a topic...")
	err = admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: int32(partitions), ReplicationFactor: 1}, false)
	if err != nil {
		logger.Error(fmt.Sprintf("creating a topic failed: %v", err))
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			os.Exit(-1)
		}
	}
}

func deleteTopic() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_7_0_0

	logger.Debug("creating cluster admin...")
	admin, err := sarama.NewClusterAdmin([]string{brokerAddress}, config)
	if err != nil {
		logger.Error(fmt.Sprintf("creating a cluster admin failed: %v", err))
	}
	defer func(admin sarama.ClusterAdmin) {
		logger.Debug("closing cluster admin...")
		err := admin.Close()
		if err != nil {
			logger.Error(fmt.Sprintf("closing the cluster admin failed: %v", err))
		}

	}(admin)

	logger.Debug("deleting topic...")
	err = admin.DeleteTopic(topic)
	if err != nil {
		logger.Error(fmt.Sprintf("deleting topic failed: %v", err))
		os.Exit(-1)
	}
}
