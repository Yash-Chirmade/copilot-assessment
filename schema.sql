CREATE DATABASE eventdb;
GO
USE eventdb;
GO

CREATE TABLE users (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128),
    email VARCHAR(128),
    created_at DATETIME
);

CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64),
    amount DECIMAL(18,2),
    status VARCHAR(32),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE payments (
    id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64),
    status VARCHAR(32),
    settled_at DATETIME,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE inventory (
    sku VARCHAR(64) PRIMARY KEY,
    quantity INT,
    adjusted_at DATETIME
);
