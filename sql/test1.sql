-- AI-generated test script

BEGIN;

-- Create tables
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    price DECIMAL(10,2)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT,
    order_date DATE,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert data into users
INSERT INTO users (user_id, username, email) VALUES
(1, 'alice', 'alice@example.com'),
(2, 'bob', 'bob@example.com'),
(3, 'carol', 'carol@example.com');

-- Insert data into products
INSERT INTO products (product_id, product_name, price) VALUES
(1, 'Laptop', 1200.00),
(2, 'Mouse', 25.50),
(3, 'Keyboard', 45.00);

-- Insert data into orders
INSERT INTO orders (order_id, user_id, product_id, quantity, order_date) VALUES
(1, 1, 1, 1, '2024-06-01'),
(2, 1, 2, 2, '2024-06-02'),
(3, 2, 3, 1, '2024-06-03'),
(4, 3, 2, 1, '2024-06-04');



-- Simple queries

-- 1. List all users
SELECT * FROM users;

-- 2. List all products with price > 30
SELECT * FROM products WHERE price > 30;

-- 3. Show all orders with user and product names
SELECT o.order_id, u.username, p.product_name, o.quantity, o.order_date
FROM orders o
JOIN users u ON o.user_id = u.user_id
JOIN products p ON o.product_id = p.product_id;

-- 4. Count total orders per user
SELECT u.username, COUNT(o.order_id) AS total_orders
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.username;

-- 5. Calculate total spent by each user
SELECT u.username, SUM(p.price * o.quantity) AS total_spent
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN products p ON o.product_id = p.product_id
GROUP BY u.username;

END;