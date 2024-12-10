package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

// Version 0.5
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("kafka application v0.3")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Rad the config file
	byteResult := ReadFile(config_file)

	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	//If not a producer, then a consumer in the config yaml
	if !configYaml.Producer {
		// Create kafka consumer configuration for kafkaCfg
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
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
		})
		if err != nil {
			fmt.Println("Failed to create consumer. ", err)
			os.Exit(1)
		}
		fmt.Println("Created Consumer. ", consumer)

		topics := []string{configYaml.Topics}
		err = consumer.SubscribeTopics(topics, nil)

		run := true
		for run {
			//fmt.Printf("waiting for kafka message\n")
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
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
					if configYaml.Timestamp {
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
	} else { //This is a producer
		fmt.Printf("Kafka producer \n")
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": configYaml.BootstrapServers,
			"sasl.mechanisms":   configYaml.SaslMechanisms,
			"security.protocol": configYaml.SecurityProtocol,
			"sasl.username":     configYaml.SaslUsername,
			"sasl.password":     configYaml.SaslPassword,
			"ssl.ca.location":   configYaml.SslCaLocation,
			"client.id":         configYaml.GroupID,
			"acks":              "all"})
		if err != nil {
			fmt.Printf("Failed to create producer: %s\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created Producer %v\n", producer)
		defer producer.Close()

		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Kafka Producer")
		fmt.Println("Insert/Paste JSON message and press enter")
		fmt.Println("CTRL-C or CTRL-Z to cancel")
		for {
			fmt.Print("-> ")
			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			fmt.Println("Message to send: ", text)
			// Convert string to serial byte format for transmission
			bytes := []byte(text)
			// Produce the message to the Kafka topic
			err = produceMessage(producer, configYaml.Topics, bytes)
			if err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
				return
			}
			fmt.Println("Message produced successfully!")
		}
	}
}

// configuration file kafka-config.yaml
type Config struct {
	Timestamp        bool   `yaml:"timestamp"`
	Producer         bool   `yaml:"producer"`
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

func produceMessage(p *kafka.Producer, topic string, message []byte) error {
	// Create a new Kafka message to be produced
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}
	// Produce the Kafka message
	deliveryChan := make(chan kafka.Event)
	err := p.Produce(kafkaMessage, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}
	// Wait for delivery report or error
	e := <-deliveryChan
	m := e.(*kafka.Message)
	// Check for delivery errors
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %s", m.TopicPartition.Error)
	}
	// Close the delivery channel
	close(deliveryChan)

	return nil
}
