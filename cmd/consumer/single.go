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
}

// Analytics represents the aggregated statistics
type Analytics struct {
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
    analyticsStore = make(map[string]*Analytics)
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

    // Context for shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle shutdown signals
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

    // Processing channels
    messagesChan := make(chan *Message, 1000)
    workerWg := &sync.WaitGroup{}

    // Start workers
    numWorkers := 4
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
            h.messagesChan <- &msg
            session.MarkMessage(message, "")
        case <-session.Context().Done():
            return nil
        }
    }
}

func worker(id int, messagesChan <-chan *Message, wg *sync.WaitGroup) {
    defer wg.Done()
    log.Printf("Worker %d started", id)

    for msg := range messagesChan {
        processMessage(msg)
    }

    log.Printf("Worker %d stopped", id)
}

func processMessage(msg *Message) {
    storeMutex.Lock()
    defer storeMutex.Unlock()

    stats, exists := analyticsStore[msg.Category]
    if !exists {
        stats = &Analytics{
            Min: msg.Value,
            Max: msg.Value,
        }
        analyticsStore[msg.Category] = stats
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
    for category, stats := range analyticsStore {
        stats.mu.RLock()
        fmt.Printf("\nCategory: %s\n", category)
        fmt.Printf("  Count: %d\n", stats.Count)
        fmt.Printf("  Average: %.2f\n", stats.Average)
        fmt.Printf("  Min: %.2f\n", stats.Min)
        fmt.Printf("  Max: %.2f\n", stats.Max)
        fmt.Printf("  Last Updated: %s\n", stats.LastUpdated.Format(time.RFC3339))
        stats.mu.RUnlock()
    }
}