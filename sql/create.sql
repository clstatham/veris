BEGIN;
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
COMMIT;