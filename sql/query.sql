BEGIN;
    SHOW TABLES;

    SELECT * FROM users;

    SELECT * FROM products;

    SELECT * FROM orders;

    SELECT * FROM products WHERE price > 30;

    SELECT o.order_id, u.username, p.product_name, o.quantity, o.order_date
    FROM orders o
    JOIN users u ON o.user_id = u.user_id
    JOIN products p ON o.product_id = p.product_id;

    SELECT u.username, COUNT(o.order_id) AS total_orders
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.username;

    SELECT u.username, SUM(p.price * o.quantity) AS total_spent
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    JOIN products p ON o.product_id = p.product_id
    GROUP BY u.username;

    SELECT p.product_name, SUM(o.quantity) AS total_sold
    FROM products p
    JOIN orders o ON p.product_id = o.product_id
    GROUP BY p.product_name
    ORDER BY total_sold DESC;

    SELECT u.username, COUNT(o.order_id) AS total_orders
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    WHERE o.order_date BETWEEN '2024-06-01' AND '2024-06-30'
    GROUP BY u.username
    ORDER BY total_orders DESC;
COMMIT;