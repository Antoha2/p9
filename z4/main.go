package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

var (
	brokers    = []string{"localhost:9092"}
	version    = sarama.DefaultVersion.String()
	group      = "test-group"
	topic      = "test-topic"
	oldest     = true
	verbose    = true
	numWorkers = 2
	//groupID    = 5
	// assignor   = "roundrobin"
)

func main() {

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Version = sarama.DefaultVersion

	createTopic(config)

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		fmt.Println("Could not create producer: ", err)
		return
	}
	defer producer.Close()

	produce(producer)

	consume()

	time.Sleep(3 * time.Second)
}

//-------------------------------------------
func produce(producer sarama.AsyncProducer) {

	go func() {
		for {
			select {
			case success := <-producer.Successes():
				log.Printf("Message sent to partition %d at offset %d", success.Partition, success.Offset)
			case err := <-producer.Errors():
				log.Printf("Failed to produce message: %v", err)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: 1,
			Value:     sarama.StringEncoder(fmt.Sprintf("Message #%d", i)),
		}
		producer.Input() <- msg
		// time.Sleep(1 * time.Second) // Добавление задержки для демонстрации последовательной отправки
	}
}

//-------------------------------------------
func consume() {
	keepRunning := true

	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready:       make(chan bool),
		numWorkers:  numWorkers,
		workerQueue: make(chan *sarama.ConsumerMessage, numWorkers),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topic, ","), &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	for i := 0; i < numWorkers; i++ {
		go consumer.worker(ctx)
	}

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, os.Interrupt)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready       chan bool
	numWorkers  int
	workerQueue chan *sarama.ConsumerMessage
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			consumer.workerQueue <- message

			//log.Printf("Message claimed: value = %s, topic = %s", string(message.Value), message.Topic)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// worker обрабатывает сообщения из workerQueue
func (consumer *Consumer) worker(ctx context.Context) {
	for {
		select {
		case msg, ok := <-consumer.workerQueue:
			if !ok {
				return
			}
			log.Printf("Worker processing message: value = %s,  topic = %s", string(msg.Value), msg.Topic)
		case <-ctx.Done():
			return
		}
	}
}

func createTopic(config *sarama.Config) error {

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return fmt.Errorf("error creating cluster admin: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("error creating topic: %v", err)
	}

	return nil
}
