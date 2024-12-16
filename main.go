package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

// Version 0.5
const config_file = "kafka-config.yaml"
const numConsumers = 4
const workerThreads = 4

var devices = map[string]string{"10.10.10.1": "host-1"}

func main() {
	fmt.Println("kafka multiple consumer v0.1")
	// Read the config file
	byteResult := ReadFile(config_file)
	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-config.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Create worker pool for processing messages in threads
	workerPool := NewWorkerPool(workerThreads) // Create a worker pool
	workerPool.Start()

	var wg sync.WaitGroup
	quit := make(chan bool)
	// Start multiple consumer workers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go consumerWorker(i, configYaml, &wg, quit, *workerPool)
	}

	sig := <-sigchan
	fmt.Printf("Caught signal %v: terminating consumers\n", sig)
	close(quit)
	wg.Wait()

	fmt.Println("All consumers stopped.")

}

func consumerWorker(id int, configYaml Config, wg *sync.WaitGroup, quit <-chan bool, workerPool WorkerPool) {
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

	defer wg.Done()
	consumer, err := kafka.NewConsumer(kafka_config)
	if err != nil {
		fmt.Printf("Consumer %d: Failed to create consumer: %v\n", id, err)
		return
	}
	defer consumer.Close()
	err = consumer.Subscribe(configYaml.Topics, nil)
	if err != nil {
		fmt.Printf("Consumer %d: Failed to subscribe to topic: %v\n", id, err)
		return
	}
	fmt.Printf("Consumer %d: Started\n", id)

	run := true
	for run {
		//fmt.Printf("waiting for kafka message\n")
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
				fmt.Printf("consumer: %d Got a kafka message\n", id)
				device := KafkaKeyCheck(e.Key, devices)
				if device != "" {
					fmt.Printf("consumer: %d Break\n", id)
					break // kafka message key device not in list terminate switch-case
				}
				var metadata Metadata
				metadata.KafakTimestamp = e.Timestamp.UnixNano() //metadata timestamp
				metadata.KafkaPartition = e.TopicPartition.Partition
				copy_e_value := make([]byte, len([]byte(e.Value)))
				copy_e_value = append([]byte(nil), []byte(e.Value)...)

				message := MessageChannel{Msg: copy_e_value, Metadata: metadata}
				// Send message to workpool goroutines
				workerPool.Submit(message) // send message&device into pool of threads

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

type Metadata struct { // Metadata from kafka
	KafakTimestamp int64 // kafka metadata timestamp
	KafkaPartition int32 // kafka partition
}

type MessageChannel struct { // Type of data to send in message channel to worker
	Msg      []byte   // Received message to process
	Metadata Metadata // kafka metadata
}

// Initialise workerpool with max thread number
func NewWorkerPool(workerCount int) *WorkerPool {
	return &WorkerPool{
		messageQueue: make(chan MessageChannel),
		workerCount:  workerCount,
	}
}

// goroutine worker pool implementation for processing messages in threads
type Worker struct {
	id           int
	messageQueue <-chan MessageChannel
}

// Start running worker thread in goroutine
func (w *Worker) Start() {
	go func() {
		for message := range w.messageQueue { //Process the message queue via channel
			//Print Message with timestamp
			fmt.Printf("From thread:%d \n", w.id)
			fmt.Printf("Partition:%+v Timestamp:%+v: %s\n", message.Metadata.KafkaPartition, message.Metadata.KafakTimestamp, message.Msg)
		}
	}()
}

type WorkerPool struct {
	messageQueue chan MessageChannel
	workerCount  int // Maximum number of workers/threads
}

// Start number of running worker threads
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		worker := Worker{id: i, messageQueue: wp.messageQueue}
		worker.Start()
	}
}

// Receives message and send to workerpool via channel
func (wp *WorkerPool) Submit(msg MessageChannel) {
	wp.messageQueue <- msg
}

// Check kafka message key against list of devices
func KafkaKeyCheck(key []byte, devices map[string]string) string {
	keyString := strings.Split(string(key), ":")
	source_ip := keyString[0]
	device, ok := devices[source_ip]
	fmt.Printf("KafkaKeycheck: %s \n", source_ip)
	if ok {
		return device // Return device if source IP in device list
	} else {
		return "" // Return nil if not in list
	}
}
