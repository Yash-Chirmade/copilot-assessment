package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

type OrderPlaced struct {
	EventID   string    `json:"eventId"`
	OrderID   string    `json:"orderId"`
	UserID    string    `json:"userId"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"createdAt"`
	Type      string    `json:"type"`
}
type UserCreated struct {
	EventID   string    `json:"eventId"`
	UserID    string    `json:"userId"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
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

type DLQPayload struct {
	Error   string `json:"error"`
	Payload string `json:"payload"`
}

func main() {

	var processedCount int64
	var dlqCount int64
	var dbLatencies []int64
	var dbLatenciesLock sync.Mutex
	ctx := context.Background()

	// Kafka
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		GroupID:  os.Getenv("KAFKA_GROUP_ID"),
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer kafkaReader.Close()

	// SQL Server
	db, err := sql.Open("mssql", os.Getenv("SQL_SERVER_DSN"))
	if err != nil {
		log.Fatalf("failed to connect to SQL Server: %v", err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	defer redisClient.Close()

	for {
		m, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			log.Printf(`{"level":"error","msg":"error reading message","error":"%v"}`, err)
			continue
		}
		var base struct {
			Type    string `json:"type"`
			EventID string `json:"eventId"`
		}
		err = json.Unmarshal(m.Value, &base)
		if err != nil {
			pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("json parse error: %v", err), base.EventID)
			atomic.AddInt64(&dlqCount, 1)
			continue
		}

		// Force error for OrderPlaced events to test DLQ
		if base.Type == "OrderPlaced" {
			pushDLQ(ctx, redisClient, m.Value, "Forced error for testing DLQ", base.EventID)
			atomic.AddInt64(&dlqCount, 1)
			continue
		}

		var eventId string
		switch base.Type {
		case "UserCreated":
			var e UserCreated
			if err := json.Unmarshal(m.Value, &e); err != nil {
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("UserCreated parse error: %v", err), base.EventID)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
			eventId = e.EventID
			var lastErr error
			success := retryUpsert(func() error {
				start := time.Now()
				err := upsertUser(ctx, db, e)
				dbLatenciesLock.Lock()
				dbLatencies = append(dbLatencies, time.Since(start).Milliseconds())
				dbLatenciesLock.Unlock()
				lastErr = err
				return err
			})
			if !success {
				log.Printf("[main] UserCreated lastErr before DLQ: %v", lastErr)
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("UserCreated DB error after retries: %v", lastErr), e.EventID)
				log.Printf("UserCreated DB error after retries: %v", lastErr)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
		case "OrderPlaced":
			var e OrderPlaced
			if err := json.Unmarshal(m.Value, &e); err != nil {
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("OrderPlaced parse error: %v", err), base.EventID)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
			eventId = e.EventID
			var lastErr error
			success := retryUpsert(func() error {
				start := time.Now()
				err := upsertOrder(ctx, db, e)
				dbLatenciesLock.Lock()
				dbLatencies = append(dbLatencies, time.Since(start).Milliseconds())
				dbLatenciesLock.Unlock()
				lastErr = err
				return err
			})
			if !success {
				log.Printf("[main] OrderPlaced lastErr before DLQ: %v", lastErr)
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("OrderPlaced DB error after retries: %v", lastErr), e.EventID)
				log.Printf("OrderPlaced DB error after retries: %v", lastErr)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
		case "PaymentSettled":
			var e PaymentSettled
			if err := json.Unmarshal(m.Value, &e); err != nil {
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("PaymentSettled parse error: %v", err), base.EventID)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
			eventId = e.EventID
			var lastErr error
			success := retryUpsert(func() error {
				start := time.Now()
				err := upsertPayment(ctx, db, e)
				dbLatenciesLock.Lock()
				dbLatencies = append(dbLatencies, time.Since(start).Milliseconds())
				dbLatenciesLock.Unlock()
				lastErr = err
				return err
			})
			if !success {
				log.Printf("[main] PaymentSettled lastErr before DLQ: %v", lastErr)
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("PaymentSettled DB error after retries: %v", lastErr), e.EventID)
				log.Printf("PaymentSettled DB error after retries: %v", lastErr)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
			// no break needed
		case "InventoryAdjusted":
			var e InventoryAdjusted
			if err := json.Unmarshal(m.Value, &e); err != nil {
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("InventoryAdjusted parse error: %v", err), base.EventID)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
			eventId = e.EventID
			var lastErr error
			success := retryUpsert(func() error {
				start := time.Now()
				err := upsertInventory(ctx, db, e)
				dbLatenciesLock.Lock()
				dbLatencies = append(dbLatencies, time.Since(start).Milliseconds())
				dbLatenciesLock.Unlock()
				lastErr = err
				return err
			})
			if !success {
				log.Printf("[main] InventoryAdjusted lastErr before DLQ: %v", lastErr)
				pushDLQ(ctx, redisClient, m.Value, fmt.Sprintf("InventoryAdjusted DB error after retries: %v", lastErr), e.EventID)
				log.Printf("InventoryAdjusted DB error after retries: %v", lastErr)
				atomic.AddInt64(&dlqCount, 1)
				continue
			}
			// no break needed
		default:
			pushDLQ(ctx, redisClient, m.Value, "unknown event type", base.EventID)
			atomic.AddInt64(&dlqCount, 1)
		}
		atomic.AddInt64(&processedCount, 1)
		log.Printf(`{"level":"info","msg":"processed event","type":"%s","eventId":"%s"}`, base.Type, eventId)
	}
}

func pushDLQ(ctx context.Context, rdb *redis.Client, payload []byte, errMsg string, eventId string) {
	dlq := DLQPayload{
		Error:   errMsg,
		Payload: string(payload),
	}
	b, _ := json.Marshal(dlq)
	rdb.LPush(ctx, "dlq", b)
	log.Printf(`{"level":"warn","msg":"pushed to DLQ","eventId":"%s","error":"%s"}`, eventId, errMsg)

}

// retryUpsert retries the given upsert function up to 3 times with a short delay.
func retryUpsert(fn func() error) bool {
	for i := 0; i < 3; i++ {
		err := fn()
		if err == nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func upsertUser(ctx context.Context, db *sql.DB, e UserCreated) error {
	res, err := db.ExecContext(ctx, `UPDATE users SET name=?, email=?, created_at=? WHERE id=?`,
		e.Name, e.Email, e.CreatedAt, e.UserID,
	)
	if err != nil {
		log.Printf("[upsertUser] UPDATE error: %v", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("[upsertUser] RowsAffected error: %v", err)
		return err
	}
	if rows == 0 {
		_, err = db.ExecContext(ctx, `INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)`,
			e.UserID, e.Name, e.Email, e.CreatedAt,
		)
		if err != nil {
			log.Printf("[upsertUser] INSERT error: %v", err)
			return err
		}
	}
	return nil
}

func upsertOrder(ctx context.Context, db *sql.DB, e OrderPlaced) error {
	var userExists int
	err := db.QueryRowContext(ctx, "SELECT COUNT(1) FROM users WHERE id=?", e.UserID).Scan(&userExists)
	if err != nil {
		log.Printf("[upsertOrder] user existence check error: %v", err)
		return fmt.Errorf("failed to check user existence: %w", err)
	}
	if userExists == 0 {
		log.Printf("[upsertOrder] user_id %s does not exist for order %s", e.UserID, e.OrderID)
		return fmt.Errorf("user_id %s does not exist for order %s", e.UserID, e.OrderID)
	}
	res, err := db.ExecContext(ctx, `UPDATE orders SET user_id=?, amount=?, status=?, created_at=? WHERE id=?`,
		e.UserID, e.Amount, "placed", e.CreatedAt, e.OrderID,
	)
	if err != nil {
		log.Printf("[upsertOrder] UPDATE error: %v", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("[upsertOrder] RowsAffected error: %v", err)
		return err
	}
	if rows == 0 {
		_, err = db.ExecContext(ctx, `INSERT INTO orders (id, user_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)`,
			e.OrderID, e.UserID, e.Amount, "placed", e.CreatedAt,
		)
		if err != nil {
			log.Printf("[upsertOrder] INSERT error: %v", err)
			return err
		}
	}
	return nil
}

func upsertPayment(ctx context.Context, db *sql.DB, e PaymentSettled) error {
	var orderExists int
	err := db.QueryRowContext(ctx, "SELECT COUNT(1) FROM orders WHERE id=?", e.OrderID).Scan(&orderExists)
	if err != nil {
		log.Printf("[upsertPayment] order existence check error: %v", err)
		return fmt.Errorf("failed to check order existence: %w", err)
	}
	if orderExists == 0 {
		log.Printf("[upsertPayment] order_id %s does not exist for payment %s", e.OrderID, e.PaymentID)
		return fmt.Errorf("order_id %s does not exist for payment %s", e.OrderID, e.PaymentID)
	}
	res, err := db.ExecContext(ctx, `UPDATE payments SET order_id=?, status=?, settled_at=? WHERE id=?`,
		e.OrderID, e.Status, e.SettledAt, e.PaymentID,
	)
	if err != nil {
		log.Printf("[upsertPayment] UPDATE error: %v", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("[upsertPayment] RowsAffected error: %v", err)
		return err
	}
	if rows == 0 {
		_, err = db.ExecContext(ctx, `INSERT INTO payments (id, order_id, status, settled_at) VALUES (?, ?, ?, ?)`,
			e.PaymentID, e.OrderID, e.Status, e.SettledAt,
		)
		if err != nil {
			log.Printf("[upsertPayment] INSERT error: %v", err)
			return err
		}
	}
	return nil
}

func upsertInventory(ctx context.Context, db *sql.DB, e InventoryAdjusted) error {
	res, err := db.ExecContext(ctx, `UPDATE inventory SET quantity=?, adjusted_at=? WHERE sku=?`,
		e.Quantity, e.AdjustedAt, e.SKU,
	)
	if err != nil {
		log.Printf("[upsertInventory] UPDATE error: %v", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("[upsertInventory] RowsAffected error: %v", err)
		return err
	}
	if rows == 0 {
		_, err = db.ExecContext(ctx, `INSERT INTO inventory (sku, quantity, adjusted_at) VALUES (?, ?, ?)`,
			e.SKU, e.Quantity, e.AdjustedAt,
		)
		if err != nil {
			log.Printf("[upsertInventory] INSERT error: %v", err)
			return err
		}
	}
	return nil
}
