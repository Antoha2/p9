package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const topic = "test-topic"

var brokers = []string{"localhost:9092"}
var numWorkers = 5

func main() {

	produce()
	consumeWithWorkers()

	time.Sleep(3 * time.Second)

}

func produce() {

	// producer, err := NewProducer()
	// if err != nil {
	// 	fmt.Println("Could not create producer: ", err)
	// 	return
	// }

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer producer.Close()

	go func() {
		for {
			select {
			case success := <-producer.Successes():
				log.Printf("Message sent to partition %d at offset %d", success.Partition, success.Offset)
				// case err := <-producer.Errors():
				// 	log.Printf("Failed to produce message: %v", err)
				// 	return
			}
		}
	}()

	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("Message #%d", i)),
		}
		producer.Input() <- msg
		time.Sleep(1 * time.Second) // Добавление задержки для демонстрации последовательной отправки
	}
}

func NewProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(brokers, config)

	return producer, err
}

func PrepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: 1,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}

// func consume(topic string) {
// 	config := sarama.NewConfig()
// 	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
// 	config.Version = sarama.V2_8_0_0

// 	group := "test-group"

// 	consumer := &Consumer{
// 		ready:       make(chan bool),
// 		numWorkers:  numWorkers,
// 		workerQueue: make(chan *sarama.ConsumerMessage, numWorkers),
// 	}

// 	ctx, cancel := context.WithCancel(context.Background())

// 	client, err := sarama.NewConsumerGroup(brokers, group, config)
// 	if err != nil {
// 		log.Fatalln("Error creating client:", err)
// 	}

// 	wg := &sync.WaitGroup{}
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			// `Consume` should be called inside an infinite loop, when a
// 			// server-side rebalance happens, the consumer session will need to be
// 			// recreated to get the new claims
// 			if err := client.Consume(ctx, []string{topic}, &consumer); err != nil {
// 				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
// 					return
// 				}
// 				log.Panicf("Error from consumer: %v", err)
// 			}
// 			// check if context was cancelled, signaling that the consumer should stop
// 			if ctx.Err() != nil {
// 				return
// 			}
// 			consumer.ready = make(chan bool)
// 		}
// 	}()

// 	// messages := make(chan sarama.ConsumerMessage, numWorkers*2)

// 	// for i := 0; i < numWorkers; i++ {
// 	// 	go func(messages chan sarama.ConsumerMessage) {
// 	// 		for message := range messages {
// 	// 			fmt.Println("Received message:", string(message.Value))
// 	// 		}
// 	// 	}(messages)
// 	// }

// 	// Consume messages and distribute them to goroutines
// 	// for {
// 	// 	messages <- <-client.Consume(context.Background(), []string{topic}, consumer)
// 	// }

// 	// client, err := sarama.NewConsumer(brokers, nil)
// 	// if err != nil {
// 	// 	fmt.Println("Could not create client: ", err)
// 	// }
// 	// partitionList, err := client.Partitions(topic) //get all partitions on the given topic
// 	// if err != nil {
// 	// 	fmt.Println("Error retrieving partitionList ", err)
// 	// }
// 	// initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

// 	// for _, partition := range partitionList {
// 	// 	pc, _ := client.ConsumePartition(topic, partition, initialOffset)

// 	// 	go func(pc sarama.PartitionConsumer) {
// 	// 		for message := range pc.Messages() {
// 	// 			messageReceived(message)
// 	// 		}
// 	// 	}(pc)
// 	// }
// }

func messageReceived(message *sarama.ConsumerMessage) {
	log.Println(string(message.Value))
}

func consumeWithWorkers() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V2_8_0_0

	client, err := sarama.NewConsumerGroup(brokers, "test-group", config)
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}

	consumer := &Consumer{
		ready:       make(chan bool),
		numWorkers:  numWorkers,
		workerQueue: make(chan *sarama.ConsumerMessage, numWorkers),
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{topic}, consumer); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			// Check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Wait until the consumer has been set up

	// Запуск воркеров
	for i := 0; i < numWorkers; i++ {
		go consumer.worker(ctx)
	}

	// Ждем завершения программы
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)
	<-sigterm
	log.Println("Terminating: via signal")

	cancel()
	wg.Wait()

	if err := client.Close(); err != nil {
		log.Fatalf("Error closing client: %v", err)
	}
}

// Consumer представляет собой Sarama consumer group consumer
type Consumer struct {
	ready       chan bool
	numWorkers  int
	workerQueue chan *sarama.ConsumerMessage
}

// Setup вызывается в начале новой сессии, перед ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup вызывается в конце сессии, после выхода всех горутин ConsumeClaim
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	close(consumer.workerQueue)
	return nil
}

// ConsumeClaim запускает цикл обработки сообщений ConsumerGroupClaim
func (consumer *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		consumer.workerQueue <- msg
		sess.MarkMessage(msg, "")
	}
	return nil
}

// worker обрабатывает сообщения из workerQueue
func (consumer *Consumer) worker(ctx context.Context) {
	for {
		select {
		case msg, ok := <-consumer.workerQueue:
			if !ok {
				return
			}
			log.Printf("Worker processing message: value = %s, timestamp = %v, topic = %s", string(msg.Value), msg.Timestamp, msg.Topic)
		case <-ctx.Done():
			return
		}
	}
}
