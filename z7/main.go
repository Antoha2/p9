package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var (
	brokers = []string{"localhost:9092"}
	topic   = "test-topic"
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
		key := fmt.Sprintf("key-%d", i%3)
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: 1,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(fmt.Sprintf("#%d message with key %s", i, key)),
		}
		producer.Input() <- msg
		// time.Sleep(1 * time.Second) // Добавление задержки для демонстрации последовательной отправки
	}

	time.Sleep(3 * time.Second)
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
