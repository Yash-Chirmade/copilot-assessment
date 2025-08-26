USE master;
GO
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'eventdb')
BEGIN
    CREATE DATABASE eventdb;
END
GO
USE eventdb;
GO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' and xtype='U')
BEGIN
CREATE TABLE users (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128),
    email VARCHAR(128),
    created_at DATETIME
);
END
GO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='orders' and xtype='U')
BEGIN
CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64),
    amount DECIMAL(18,2),
    status VARCHAR(32),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
END
GO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='payments' and xtype='U')
BEGIN
CREATE TABLE payments (
    id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64),
    status VARCHAR(32),
    settled_at DATETIME,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);
END
GO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventory' and xtype='U')
BEGIN
CREATE TABLE inventory (
    sku VARCHAR(64) PRIMARY KEY,
    quantity INT,
    adjusted_at DATETIME
);
END
GO
