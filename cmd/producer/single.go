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

func main() {
    // Kafka configuration
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Retry.Max = 5
    config.Producer.Return.Successes = true

    // Create producer
    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Error creating producer: %v", err)
    }
    defer producer.Close()

    // Channel for shutdown signaling
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

    // WaitGroup for goroutines
    var wg sync.WaitGroup
    done := make(chan bool)

    // Start multiple producer goroutines
    numProducers := 3
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
                ID:        fmt.Sprintf("msg-%d-%d", producerID, messageCount),
                Timestamp: time.Now(),
                Value:     rand.Float64() * 100,
                Category:  categories[rand.Intn(len(categories))],
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
                Key:   sarama.StringEncoder(msg.Category),
            }

            // Send message
            partition, offset, err := producer.SendMessage(kafkaMsg)
            if err != nil {
                log.Printf("Error sending message: %v", err)
                continue
            }

            if messageCount%100 == 0 {
                log.Printf("Producer %d: Message sent successfully! Partition: %d, Offset: %d\n",
                    producerID, partition, offset)
            }

            messageCount++
        }
    }
}