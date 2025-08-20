package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type UserCreated struct {
	EventID   string    `json:"eventId"`
	UserID    string    `json:"userId"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"createdAt"`
	Type      string    `json:"type"`
}

type OrderPlaced struct {
	EventID   string    `json:"eventId"`
	OrderID   string    `json:"orderId"`
	UserID    string    `json:"userId"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"createdAt"`
	Type      string    `json:"type"`
}

type PaymentSettled struct {
	EventID   string    `json:"eventId"`
	PaymentID string    `json:"paymentId"`
	OrderID   string    `json:"orderId"`
	Status    string    `json:"status"`
	SettledAt time.Time `json:"settledAt"`
	Type      string    `json:"type"`
}

type InventoryAdjusted struct {
	EventID    string    `json:"eventId"`
	SKU        string    `json:"sku"`
	Quantity   int       `json:"quantity"`
	AdjustedAt time.Time `json:"adjustedAt"`
	Type       string    `json:"type"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	topic := "events"
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})
	defer w.Close()

	eventTypes := []string{"UserCreated", "OrderPlaced", "PaymentSettled", "InventoryAdjusted"}
	for i := 0; i < 20; i++ {
		t := eventTypes[rand.Intn(len(eventTypes))]
		var key string
		var payload []byte
		switch t {
		case "UserCreated":
			e := UserCreated{
				EventID:   randomID(),
				UserID:    randomID(),
				Name:      randomName(),
				Email:     randomEmail(),
				CreatedAt: time.Now(),
				Type:      t,
			}
			key = e.UserID
			payload, _ = json.Marshal(e)
		case "OrderPlaced":
			e := OrderPlaced{
				EventID:   randomID(),
				OrderID:   randomID(),
				UserID:    randomID(),
				Amount:    rand.Float64() * 100,
				CreatedAt: time.Now(),
				Type:      t,
			}
			key = e.OrderID
			payload, _ = json.Marshal(e)
		case "PaymentSettled":
			e := PaymentSettled{
				EventID:   randomID(),
				PaymentID: randomID(),
				OrderID:   randomID(),
				Status:    "settled",
				SettledAt: time.Now(),
				Type:      t,
			}
			key = e.PaymentID
			payload, _ = json.Marshal(e)
		case "InventoryAdjusted":
			e := InventoryAdjusted{
				EventID:    randomID(),
				SKU:        randomID(),
				Quantity:   rand.Intn(100),
				AdjustedAt: time.Now(),
				Type:       t,
			}
			key = e.SKU
			payload, _ = json.Marshal(e)
		}
		err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(key),
			Value: payload,
		})
		if err != nil {
			log.Printf("failed to write message: %v", err)
		} else {
			log.Printf("produced %s event: %s", t, key)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func randomID() string {
	return RandString(8)
}

func randomName() string {
	names := []string{"Alice", "Bob", "Carol", "Dave"}
	return names[rand.Intn(len(names))]
}

func randomEmail() string {
	domains := []string{"example.com", "test.com"}
	return randomName() + "@" + domains[rand.Intn(len(domains))]
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
