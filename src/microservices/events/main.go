package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Topics
const (
	TopicUserEvents    = "user-events"
	TopicPaymentEvents = "payment-events"
	TopicMovieEvents   = "movie-events"
)

// Event payloads — IDs are int because tests send numeric values ({{movieId}}, {{userId}}, etc.)

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"` // registered, login, logout
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"` // initiated, completed, failed
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

type MovieEvent struct {
	MovieID   int    `json:"movie_id"`
	UserID    int    `json:"user_id"`
	Title     string `json:"title"`
	Action    string `json:"action"` // viewed, rated, added_to_watchlist
	Timestamp string `json:"timestamp"`
}

var kafkaBroker string

func main() {
	kafkaBroker = os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	// Wait for Kafka to be ready
	waitForKafka()

	// Start consumers in background goroutines
	go consumeEvents(TopicUserEvents, "events-service-users")
	go consumeEvents(TopicPaymentEvents, "events-service-payments")
	go consumeEvents(TopicMovieEvents, "events-service-movies")

	// HTTP routes
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)
	http.HandleFunc("/api/events/movie", handleMovieEvent)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Events service starting on port %s, kafka broker: %s", port, kafkaBroker)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// waitForKafka retries connecting to Kafka until it's available.
func waitForKafka() {
	for i := 0; i < 30; i++ {
		conn, err := kafka.Dial("tcp", kafkaBroker)
		if err == nil {
			conn.Close()
			log.Println("Kafka is ready")
			return
		}
		log.Printf("Waiting for Kafka (%d/30): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	log.Println("Warning: could not verify Kafka readiness, proceeding anyway")
}

// produce sends a JSON-encoded payload to the given Kafka topic.
func produce(topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(topic),
			Value: data,
		},
	)
	if err != nil {
		return err
	}
	log.Printf("[PRODUCER] topic=%s payload=%s", topic, data)
	return nil
}

// consumeEvents continuously reads from a Kafka topic and logs each message.
func consumeEvents(topic, groupID string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()

	log.Printf("[CONSUMER] Listening on topic=%s group=%s", topic, groupID)
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[CONSUMER] error reading topic=%s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("[CONSUMER] topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
}

// --- HTTP handlers ---

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Test expects: jsonData.status === true (boolean)
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ev UserEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	if err := produce(TopicUserEvents, ev); err != nil {
		log.Printf("[ERROR] produce user event: %v", err)
		http.Error(w, "failed to produce event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success", "topic": TopicUserEvents})
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ev PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	if err := produce(TopicPaymentEvents, ev); err != nil {
		log.Printf("[ERROR] produce payment event: %v", err)
		http.Error(w, "failed to produce event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success", "topic": TopicPaymentEvents})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ev MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	if err := produce(TopicMovieEvents, ev); err != nil {
		log.Printf("[ERROR] produce movie event: %v", err)
		http.Error(w, "failed to produce event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "success", "topic": TopicMovieEvents})
}
