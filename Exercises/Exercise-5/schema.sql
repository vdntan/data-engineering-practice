DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS products;

CREATE TABLE accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    address_1 VARCHAR(200),
    address_2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    join_date DATE
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(10),
    product_description VARCHAR(200)
);

CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_date DATE,
    product_id INT,
    product_code VARCHAR(10),
    product_description VARCHAR(200),
    quantity INT,
    account_id INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
);

CREATE INDEX idx_transactions_date ON transactions(transaction_date);
