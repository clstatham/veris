BEGIN;
    INSERT INTO users (user_id, username, email) VALUES
    (1, 'alice', 'alice@example.com'),
    (2, 'bob', 'bob@example.com'),
    (3, 'carol', 'carol@example.com'),
    (4, 'dave', 'dave@example.com'),
    (5, 'eve', 'eve@example.com');

    INSERT INTO products (product_id, product_name, price) VALUES
    (1, 'Laptop', 1200.00),
    (2, 'Mouse', 25.50),
    (3, 'Keyboard', 45.00),
    (4, 'Monitor', 300.00),
    (5, 'Headphones', 80.00),
    (6, 'Webcam', 60.00),
    (7, 'Microphone', 150.00),
    (8, 'Desk', 200.00),
    (9, 'Chair', 150.00),
    (10, 'USB Hub', 20.00);


    INSERT INTO orders (order_id, user_id, product_id, quantity, order_date) VALUES
    (1, 1, 1, 1, '2024-06-01'),
    (2, 1, 2, 2, '2024-06-02'),
    (3, 2, 3, 1, '2024-06-03'),
    (4, 3, 2, 1, '2024-06-04'),
    (5, 4, 4, 1, '2024-06-05'),
    (6, 5, 5, 1, '2024-06-06'),
    (7, 1, 6, 1, '2024-06-07'),
    (8, 2, 7, 1, '2024-06-08'),
    (9, 3, 8, 1, '2024-06-09'),
    (10, 4, 9, 1, '2024-06-10'),
    (11, 5, 10, 1, '2024-06-11'),
    (12, 1, 1, 1, '2024-06-12'),
    (13, 2, 2, 1, '2024-06-13'),
    (14, 3, 3, 1, '2024-06-14'),
    (15, 4, 4, 1, '2024-06-15'),
    (16, 5, 5, 1, '2024-06-16'),
    (17, 1, 6, 1, '2024-06-17'),
    (18, 2, 7, 1, '2024-06-18'),
    (19, 3, 8, 1, '2024-06-19'),
    (20, 4, 9, 1, '2024-06-20');
COMMIT;