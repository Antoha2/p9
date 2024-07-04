package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/tools/tls"
)

var (
	brokerList    = "localhost:9092"
	topic         = "test-topic"
	partitions    = "all"
	offset        = "oldest"
	verbose       = true
	tlsEnabled    = false
	tlsSkipVerify = false
	tlsClientCert = "tls-client-cert"
	tlsClientKey  = "tls-client-key"

	bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {

	if verbose {
		sarama.Logger = logger
	}

	var initialOffset int64
	switch offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	config := sarama.NewConfig()
	if tlsEnabled {
		tlsConfig, err := tls.NewConfig(tlsClientCert, tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "Failed to create TLS config: %s", err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Config.InsecureSkipVerify = tlsSkipVerify
	}

	c, err := sarama.NewConsumer([]string{brokerList}, config)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			printErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))
			fmt.Printf("Value:\t%s\n", string(msg.Value))
			fmt.Println()
		}
	}()

	wg.Wait()
	logger.Println("Done consuming topic", topic)
	close(messages)

	if err := c.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
