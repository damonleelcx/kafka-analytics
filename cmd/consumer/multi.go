package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    "github.com/IBM/sarama"
)

// Message represents the incoming data structure
type Message struct {
    ID        string    `json:"id"`
    Timestamp time.Time `json:"timestamp"`
    Value     float64   `json:"value"`
    Category  string    `json:"category"`
    Partition int32     `json:"partition"`
}

// Analytics represents the aggregated statistics per partition
type Analytics struct {
    Category    string
    Partition   int32
    Count       int64
    Sum         float64
    Average     float64
    Min         float64
    Max         float64
    LastUpdated time.Time
    mu          sync.RWMutex
}

// Global analytics store
var (
    analyticsStore = make(map[string]map[int32]*Analytics)
    storeMutex    sync.RWMutex
)

func main() {
    // Kafka configuration
    config := sarama.NewConfig()
    config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
    config.Consumer.Offsets.Initial = sarama.OffsetOldest

    // Create consumer group
    group, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "analytics-group", config)
    if err != nil {
        log.Fatalf("Error creating consumer group: %v", err)
    }
    defer group.Close()

    // Initialize analytics store for each category and partition
    categories := []string{"sales", "traffic", "users", "errors", "latency"}
    storeMutex.Lock()
    for _, category := range categories {
        analyticsStore[category] = make(map[int32]*Analytics)
        for partition := int32(0); partition < 5; partition++ {
            analyticsStore[category][partition] = &Analytics{
                Category:  category,
                Partition: partition,
            }
        }
    }
    storeMutex.Unlock()

    // Context for shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle shutdown signals
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

    // Processing channels
    messagesChan := make(chan *Message, 1000)
    workerWg := &sync.WaitGroup{}

    // Start workers - one worker per partition
    numWorkers := 5
    for i := 0; i < numWorkers; i++ {
        workerWg.Add(1)
        go worker(i, messagesChan, workerWg)
    }

    // Start analytics reporter
    go reportAnalytics(ctx)

    // Consumer group handler
    handler := &ConsumerGroupHandler{
        messagesChan: messagesChan,
        ready:        make(chan bool),
    }

    // Consumer loop
    topics := []string{"analytics-topic"}
    consumeWg := &sync.WaitGroup{}
    consumeWg.Add(1)
    go func() {
        defer consumeWg.Done()
        for {
            if err := group.Consume(ctx, topics, handler); err != nil {
                log.Printf("Error from consumer: %v", err)
            }
            if ctx.Err() != nil {
                return
            }
            handler.ready = make(chan bool)
        }
    }()

    // Wait for signal
    <-signals
    fmt.Println("\nShutdown signal received...")
    cancel()
    
    // Wait for all operations to complete
    consumeWg.Wait()
    close(messagesChan)
    workerWg.Wait()
}

type ConsumerGroupHandler struct {
    messagesChan chan<- *Message
    ready        chan bool
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
    close(h.ready)
    return nil
}

func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
    return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    log.Printf("Starting to consume partition %d\n", claim.Partition())
    for {
        select {
        case message, ok := <-claim.Messages():
            if !ok {
                return nil
            }
            var msg Message
            if err := json.Unmarshal(message.Value, &msg); err != nil {
                log.Printf("Error unmarshaling message: %v", err)
                continue
            }
            msg.Partition = message.Partition
            h.messagesChan <- &msg
            session.MarkMessage(message, "")
        case <-session.Context().Done():
            return nil
        }
    }
}

func worker(id int, messagesChan <-chan *Message, wg *sync.WaitGroup) {
    defer wg.Done()
    log.Printf("Worker %d started for partition %d", id, id)

    for msg := range messagesChan {
        processMessage(msg)
    }

    log.Printf("Worker %d stopped", id)
}

func processMessage(msg *Message) {
    storeMutex.Lock()
    defer storeMutex.Unlock()

    categoryStats, exists := analyticsStore[msg.Category]
    if !exists {
        categoryStats = make(map[int32]*Analytics)
        analyticsStore[msg.Category] = categoryStats
    }

    stats, exists := categoryStats[msg.Partition]
    if !exists {
        stats = &Analytics{
            Category:  msg.Category,
            Partition: msg.Partition,
            Min:       msg.Value,
            Max:       msg.Value,
        }
        categoryStats[msg.Partition] = stats
    }

    stats.mu.Lock()
    defer stats.mu.Unlock()

    stats.Count++
    stats.Sum += msg.Value
    stats.Average = stats.Sum / float64(stats.Count)
    stats.LastUpdated = msg.Timestamp

    if msg.Value < stats.Min {
        stats.Min = msg.Value
    }
    if msg.Value > stats.Max {
        stats.Max = msg.Value
    }
}

func reportAnalytics(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            printAnalytics()
        }
    }
}

func printAnalytics() {
    storeMutex.RLock()
    defer storeMutex.RUnlock()

    fmt.Println("\n=== Analytics Report ===")
    for category, partitionStats := range analyticsStore {
        fmt.Printf("\nCategory: %s\n", category)
        for partition, stats := range partitionStats {
            stats.mu.RLock()
            fmt.Printf("  Partition %d:\n", partition)
            fmt.Printf("    Count: %d\n", stats.Count)
            fmt.Printf("    Average: %.2f\n", stats.Average)
            fmt.Printf("    Min: %.2f\n", stats.Min)
            fmt.Printf("    Max: %.2f\n", stats.Max)
            fmt.Printf("    Last Updated: %s\n", stats.LastUpdated.Format(time.RFC3339))
            stats.mu.RUnlock()
        }
    }
}