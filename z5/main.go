package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var (
	brokers       = []string{"localhost:9092"}
	topic         = "test-topic"
	numPartitions = 3
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
		produce(producer)
	}()

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

	for j := 0; j < int(numPartitions); j++ {
		for i := 0; i < 10; i++ {
			msg := &sarama.ProducerMessage{
				Topic:     topic,
				Partition: int32(j),
				Value:     sarama.StringEncoder(fmt.Sprintf("Message #%d", i)),
			}
			producer.Input() <- msg
			//time.Sleep(1 * time.Second) // Добавление задержки для демонстрации последовательной отправки
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
		NumPartitions:     int32(numPartitions),
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("error creating topic: %v", err)
	}

	return nil
}
