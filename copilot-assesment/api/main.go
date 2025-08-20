package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-chi/chi/v5"
)

type User struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"createdAt"`
	Orders    []Order   `json:"orders,omitempty"`
}

type Order struct {
	ID        string    `json:"id"`
	UserID    string    `json:"userId"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	Payment   *Payment  `json:"payment,omitempty"`
}

type Payment struct {
	ID        string    `json:"id"`
	OrderID   string    `json:"orderId"`
	Status    string    `json:"status"`
	SettledAt time.Time `json:"settledAt"`
}

func main() {
	db, err := sql.Open("sqlserver", os.Getenv("SQLSERVER_CONN"))
	if err != nil {
		log.Fatalf("failed to connect to SQL Server: %v", err)
	}
	defer db.Close()

	r := chi.NewRouter()
	r.Get("/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		user, err := getUserWithOrders(r.Context(), db, id)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(user)
	})

	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		order, err := getOrderWithPayment(r.Context(), db, id)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(order)
	})

	log.Println("API server running on :8080")
	http.ListenAndServe(":8080", r)
}

func getUserWithOrders(ctx context.Context, db *sql.DB, id string) (*User, error) {
	var user User
	err := db.QueryRowContext(ctx, `SELECT id, name, email, created_at FROM users WHERE id = @p1`, sql.Named("p1", id)).Scan(&user.ID, &user.Name, &user.Email, &user.CreatedAt)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, user_id, amount, status, created_at FROM orders WHERE user_id = @p1 ORDER BY created_at DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY`, sql.Named("p1", id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Amount, &o.Status, &o.CreatedAt); err == nil {
			user.Orders = append(user.Orders, o)
		}
	}
	return &user, nil
}

func getOrderWithPayment(ctx context.Context, db *sql.DB, id string) (*Order, error) {
	var order Order
	err := db.QueryRowContext(ctx, `SELECT id, user_id, amount, status, created_at FROM orders WHERE id = @p1`, sql.Named("p1", id)).Scan(&order.ID, &order.UserID, &order.Amount, &order.Status, &order.CreatedAt)
	if err != nil {
		return nil, err
	}
	var payment Payment
	err = db.QueryRowContext(ctx, `SELECT id, order_id, status, settled_at FROM payments WHERE order_id = @p1`, sql.Named("p1", id)).Scan(&payment.ID, &payment.OrderID, &payment.Status, &payment.SettledAt)
	if err == nil {
		order.Payment = &payment
	}
	return &order, nil
}
