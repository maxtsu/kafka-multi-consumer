package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

// Version 0.5
const config_file = "kafka-config.yaml"
const numConsumers = 3

func main() {
	fmt.Println("kafka multiple consumer v0.1")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Read the config file
	byteResult := ReadFile(config_file)

	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)
	kafka_config := &kafka.ConfigMap{
		"bootstrap.servers":  configYaml.BootstrapServers,
		"sasl.mechanisms":    configYaml.SaslMechanisms,
		"security.protocol":  configYaml.SecurityProtocol,
		"sasl.username":      configYaml.SaslUsername,
		"sasl.password":      configYaml.SaslPassword,
		"ssl.ca.location":    configYaml.SslCaLocation,
		"group.id":           configYaml.GroupID,
		"session.timeout.ms": 6000,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": configYaml.AutoOffset,
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": false,
	}

	var wg sync.WaitGroup
	quit := make(chan bool)
	// Start multiple consumer workers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go consumerWorker(i, kafka_config, configYaml, &wg, quit)
	}

	close(quit)
	wg.Wait()

	fmt.Println("All consumers stopped.")

}

func createConsumer(config *kafka.ConfigMap) (*kafka.Consumer, error) {
	return kafka.NewConsumer(config)
}

func consumerWorker(id int, config *kafka.ConfigMap, config_file Config, wg *sync.WaitGroup, quit <-chan bool) {
	defer wg.Done()
	consumer, err := createConsumer(config)
	if err != nil {
		fmt.Printf("Consumer %d: Failed to create consumer: %v\n", id, err)
		return
	}
	defer consumer.Close()
	err = consumer.Subscribe(config_file.Topics, nil)
	if err != nil {
		fmt.Printf("Consumer %d: Failed to subscribe to topic: %v\n", id, err)
		return
	}
	fmt.Printf("Consumer %d: Started\n", id)

	run := true
	for run {
		fmt.Printf("waiting for kafka message\n")
		select {
		case <-quit:
			fmt.Printf("Terminating consumer %d\n", id)
			run = false
		default:
			// Poll the consumer for messages or events
			event := consumer.Poll(400)
			if event == nil {
				continue
			}
			switch e := event.(type) {
			case *kafka.Message:
				// Process the message received.
				//fmt.Printf("Got a kafka message\n")
				kafkaMessage := string(e.Value)
				if config_file.Timestamp {
					timestamp := (time.Now()).UnixMilli()
					metaDataTimestamp := e.Timestamp.UnixNano() //metadata timestamp
					partition := e.TopicPartition
					//Print Message with timestamp
					fmt.Printf("%+v: %+v: %s %d %s\n", timestamp, partition, e.Key, metaDataTimestamp, kafkaMessage)
					//fmt.Printf("%+v: %+v %s\n", timestamp, partition, e.Key) //Reduced output of keys only
				} else {
					fmt.Printf("%s\n", kafkaMessage) //Message in single string
				}
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := consumer.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				// Errors are informational, the client will try to
				// automatically recover.
				fmt.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					fmt.Printf("Kafka error. All brokers down ")
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}

// configuration file kafka-config.yaml
type Config struct {
	Timestamp        bool   `yaml:"timestamp"`
	BootstrapServers string `yaml:"bootstrap.servers"`
	SaslMechanisms   string `yaml:"sasl.mechanisms"`
	SecurityProtocol string `yaml:"security.protocol"`
	SaslUsername     string `yaml:"sasl.username"`
	SaslPassword     string `yaml:"sasl.password"`
	SslCaLocation    string `yaml:"ssl.ca.location"`
	GroupID          string `yaml:"group.id"`
	Topics           string `yaml:"topics"`
	AutoOffset       string `yaml:"auto.offset.reset"`
}

// Function to read text file return byteResult
func ReadFile(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	byteResult, _ := io.ReadAll(file)
	file.Close()
	return byteResult
}
