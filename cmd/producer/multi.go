package main

import (
    "encoding/json"
    "fmt"
    "log"
    "math/rand"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    "github.com/IBM/sarama"
)

// Message represents the data structure we'll send to Kafka
type Message struct {
    ID        string    `json:"id"`
    Timestamp time.Time `json:"timestamp"`
    Value     float64   `json:"value"`
    Category  string    `json:"category"`
}

// CustomPartitioner ensures messages from the same category go to the same partition
type CustomPartitioner struct {
    numPartitions int32
}

func (p *CustomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
    if p.numPartitions == 0 {
        p.numPartitions = numPartitions
    }

    // Get category from message key
    key, _ := message.Key.Encode()
    category := string(key)

    // Use consistent hashing to assign categories to partitions
    var partition int32
    switch category {
    case "sales":
        partition = 0
    case "traffic":
        partition = 1
    case "users":
        partition = 2
    case "errors":
        partition = 3
    case "latency":
        partition = 4
    default:
        partition = rand.Int31n(numPartitions)
    }

    // Ensure partition is within valid range
    return partition % numPartitions, nil
}

func (*CustomPartitioner) RequiresConsistency() bool {
    return true
}

func NewCustomPartitioner(topic string) sarama.Partitioner {
    return &CustomPartitioner{}
}

func main() {
    rand.Seed(time.Now().UnixNano())

    // Kafka configuration
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Retry.Max = 5
    config.Producer.Return.Successes = true
    config.Producer.Partitioner = NewCustomPartitioner

    // Create producer
    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Error creating producer: %v", err)
    }
    defer producer.Close()

    // Get metadata about the topic to verify number of partitions
    admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Error creating admin client: %v", err)
    }
    defer admin.Close()

    metadata, err := admin.DescribeTopics([]string{"analytics-topic"})
    if err != nil {
        log.Fatalf("Error describing topics: %v", err)
    }

    if len(metadata) == 0 {
        log.Fatal("No topic metadata received")
    }

    numPartitions := len(metadata[0].Partitions)
    log.Printf("Topic analytics-topic has %d partitions", numPartitions)

    // Channel for shutdown signaling
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

    // WaitGroup for goroutines
    var wg sync.WaitGroup
    done := make(chan bool)

    // Start producer goroutines
    numProducers := 5 // One producer per category
    for i := 0; i < numProducers; i++ {
        wg.Add(1)
        go func(producerID int) {
            defer wg.Done()
            produceMessages(producer, producerID, done)
        }(i)
    }

    // Wait for shutdown signal
    <-signals
    fmt.Println("\nShutdown signal received, closing producers...")
    close(done)
    wg.Wait()
}

func produceMessages(producer sarama.SyncProducer, producerID int, done chan bool) {
    categories := []string{"sales", "traffic", "users", "errors", "latency"}
    category := categories[producerID%len(categories)]
    
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    messageCount := 0

    for {
        select {
        case <-done:
            return
        case <-ticker.C:
            // Create message
            msg := Message{
                ID:        fmt.Sprintf("msg-%s-%d", category, messageCount),
                Timestamp: time.Now(),
                Value:     rand.Float64() * 100,
                Category:  category,
            }

            // Convert to JSON
            jsonData, err := json.Marshal(msg)
            if err != nil {
                log.Printf("Error marshaling message: %v", err)
                continue
            }

            // Create Kafka message
            kafkaMsg := &sarama.ProducerMessage{
                Topic: "analytics-topic",
                Value: sarama.StringEncoder(jsonData),
                Key:   sarama.StringEncoder(category),
            }

            // Send message
            partition, offset, err := producer.SendMessage(kafkaMsg)
            if err != nil {
                log.Printf("Error sending message: %v", err)
                continue
            }

            if messageCount%100 == 0 {
                log.Printf("Producer %d (Category: %s): Message sent! Partition: %d, Offset: %d\n",
                    producerID, category, partition, offset)
            }

            messageCount++
        }
    }
}