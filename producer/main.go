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

	// Step 1: Create users
	userCount := 5
	orderPerUser := 2
	paymentPerOrder := 1
	var userIDs []string
	var orderIDs []string
	for i := 0; i < userCount; i++ {
		userID := randomID()
		userIDs = append(userIDs, userID)
		e := UserCreated{
			EventID:   randomID(),
			UserID:    userID,
			Name:      randomName(),
			Email:     randomEmail(),
			CreatedAt: time.Now(),
			Type:      "UserCreated",
		}
		payload, _ := json.Marshal(e)
		err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(userID),
			Value: payload,
		})
		if err != nil {
			log.Printf("failed to write UserCreated: %v", err)
		} else {
			log.Printf("produced UserCreated event: %s", userID)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Step 2: Create orders for each user
	for _, userID := range userIDs {
		for j := 0; j < orderPerUser; j++ {
			orderID := randomID()
			orderIDs = append(orderIDs, orderID)
			e := OrderPlaced{
				EventID:   randomID(),
				OrderID:   orderID,
				UserID:    userID,
				Amount:    rand.Float64() * 100,
				CreatedAt: time.Now(),
				Type:      "OrderPlaced",
			}
			payload, _ := json.Marshal(e)
			err := w.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(orderID),
				Value: payload,
			})
			if err != nil {
				log.Printf("failed to write OrderPlaced: %v", err)
			} else {
				log.Printf("produced OrderPlaced event: %s (user %s)", orderID, userID)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Step 3: Create payments for each order
	for _, orderID := range orderIDs {
		for k := 0; k < paymentPerOrder; k++ {
			paymentID := randomID()
			e := PaymentSettled{
				EventID:   randomID(),
				PaymentID: paymentID,
				OrderID:   orderID,
				Status:    "settled",
				SettledAt: time.Now(),
				Type:      "PaymentSettled",
			}
			payload, _ := json.Marshal(e)
			err := w.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(paymentID),
				Value: payload,
			})
			if err != nil {
				log.Printf("failed to write PaymentSettled: %v", err)
			} else {
				log.Printf("produced PaymentSettled event: %s (order %s)", paymentID, orderID)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Step 4: Inventory events (remain random)
	for i := 0; i < 10; i++ {
		sku := randomID()
		e := InventoryAdjusted{
			EventID:    randomID(),
			SKU:        sku,
			Quantity:   rand.Intn(100),
			AdjustedAt: time.Now(),
			Type:       "InventoryAdjusted",
		}
		payload, _ := json.Marshal(e)
		err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(sku),
			Value: payload,
		})
		if err != nil {
			log.Printf("failed to write InventoryAdjusted: %v", err)
		} else {
			log.Printf("produced InventoryAdjusted event: %s", sku)
		}
		time.Sleep(200 * time.Millisecond)
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
