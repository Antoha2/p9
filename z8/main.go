package main

import (
	"fmt"
	"log"
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

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		fmt.Println("Could not create producer: ", err)
		return
	}
	defer producer.Close()

	produce(producer)

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}
	defer consumer.Close()

	produceWithKey(topic, consumer)

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
		key := fmt.Sprintf("key-%d", i%3)
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: 1,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(fmt.Sprintf("#%d message with key %s", i, key)),
		}
		producer.Input() <- msg
	}

}

//-------------------------------------------
func produceWithKey(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)
			}
		}(pc)
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	fmt.Printf("Message received: key = %s, value = %s\n", string(message.Key), string(message.Value))
}
