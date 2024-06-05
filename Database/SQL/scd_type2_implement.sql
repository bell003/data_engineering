/*
Query type: SCD Type 2 implement
Database server: Microsoft SQL Server
*/
-- SQL_1
-- Create a source table
CREATE TABLE customers_source (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    address VARCHAR(255),
    email VARCHAR(255),
    modified_date DATE
);

-- SQL_2
-- Create a target table
CREATE TABLE customers_target (
    surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT,
    name VARCHAR(255),
    address VARCHAR(255),
    email VARCHAR(255),
    start_date DATE,
    end_date DATE,
    is_active BIT,
    CONSTRAINT uc_customer UNIQUE (customer_id, start_date)
);

-- let's implement the insert, update practically
-- SQL_3
-- Insert the below records into the source
INSERT INTO customers_source (customer_id, name, address, email, modified_date) VALUES
(1, 'John Doe', '123 Elm St', 'john@example.com', '2024-01-01'),
(2, 'Jane Smith', '456 Oak St', 'jane@example.com', '2024-01-02'),
(3, 'Bob Johnson', '789 Pine St', 'bob@example.com', '2024-01-03');

-- SQL_4
-- Step 1: Insert New Records
INSERT INTO customers_target (customer_id, name, address, email, start_date, end_date, is_active)
SELECT s.customer_id, s.name, s.address, s.email, s.modified_date, NULL, 1
FROM customers_source s
LEFT JOIN customers_target t ON s.customer_id = t.customer_id AND t.is_active = 1
WHERE t.customer_id IS NULL;

-- Step 2: Update Existing Records
-- Step 2a: Update end_date and is_active of the current active record
UPDATE t
SET t.end_date = s.modified_date, t.is_active = 0
FROM customers_target t
JOIN customers_source s ON t.customer_id = s.customer_id
WHERE t.is_active = 1 AND (t.address != s.address OR t.email != s.email);

-- Step 2b: Insert the modified record
INSERT INTO customers_target (customer_id, name, address, email, start_date, end_date, is_active)
SELECT s.customer_id, s.name, s.address, s.email, s.modified_date, NULL, 1
FROM customers_source s
JOIN customers_target t ON s.customer_id = t.customer_id
WHERE t.is_active = 0 AND t.end_date = s.modified_date;

-- Step 3: Inactivate Deleted Records
UPDATE t
SET t.end_date = GETDATE(), t.is_active = 0
FROM customers_target t
LEFT JOIN customers_source s ON t.customer_id = s.customer_id
WHERE s.customer_id IS NULL AND t.is_active = 1;







