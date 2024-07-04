package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const topic = "test-topic"

var brokers = []string{"localhost:9092"}

func main() {

	producer, err := NewProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
		return
	}

	for i := 0; i < 10; i++ {
		msg := PrepareMessage(topic, fmt.Sprintf("Message #%d", i))
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Printf("%s error occured.", err.Error())
		} else {
			fmt.Printf("Message was saved to partion: %d.\nMessage offset is: %d.\n", partition, offset)
		}
	}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}

	subscribe(topic, consumer)
	time.Sleep(3 * time.Second)

}

func NewProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func PrepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}

func subscribe(topic string, consumer sarama.Consumer) {
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
	log.Println(string(message.Value))
}
