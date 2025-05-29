-- Create test database and tables
USE testdb;

-- Create a small table for ALTER TABLE testing (below threshold)
CREATE TABLE small_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert small amount of data (below threshold)
INSERT INTO small_table (name, email, age) VALUES
('Alice Johnson', 'alice@example.com', 25),
('Bob Smith', 'bob@example.com', 30),
('Charlie Brown', 'charlie@example.com', 35),
('Diana Prince', 'diana@example.com', 28),
('Eve Wilson', 'eve@example.com', 32);

-- Create a large table for pt-online-schema-change testing (above threshold)
CREATE TABLE large_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2),
    quantity INT DEFAULT 1,
    description TEXT,
    status ENUM('active', 'inactive', 'pending') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_category (category),
    INDEX idx_status (status)
);

-- Insert large amount of data (above threshold)
-- This will create approximately 10,000 records
INSERT INTO large_table (user_id, product_name, category, price, quantity, description, status)
SELECT
    FLOOR(1 + RAND() * 1000) as user_id,
    CONCAT('Product ', LPAD(ROW_NUMBER() OVER (), 5, '0')) as product_name,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Books'
        WHEN 3 THEN 'Home & Garden'
        ELSE 'Sports'
    END as category,
    ROUND(10 + RAND() * 990, 2) as price,
    FLOOR(1 + RAND() * 10) as quantity,
    CONCAT('Description for product ', ROW_NUMBER() OVER ()) as description,
    CASE FLOOR(RAND() * 3)
        WHEN 0 THEN 'active'
        WHEN 1 THEN 'inactive'
        ELSE 'pending'
    END as status
FROM
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION
     SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION
     SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t2,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION
     SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t3,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION
     SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t4
LIMIT 10000;

-- Create a table for testing various data types
CREATE TABLE data_types_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    varchar_col VARCHAR(255),
    text_col TEXT,
    int_col INT,
    bigint_col BIGINT,
    decimal_col DECIMAL(10,2),
    float_col FLOAT,
    double_col DOUBLE,
    date_col DATE,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    json_col JSON,
    enum_col ENUM('option1', 'option2', 'option3'),
    set_col SET('tag1', 'tag2', 'tag3'),
    binary_col BINARY(16),
    varbinary_col VARBINARY(255)
);

-- Insert sample data for data types table
INSERT INTO data_types_table (
    varchar_col, text_col, int_col, bigint_col, decimal_col, float_col, double_col,
    date_col, datetime_col, json_col, enum_col, set_col, binary_col, varbinary_col
) VALUES
('Sample text', 'This is a longer text field', 42, 9223372036854775807, 123.45, 3.14, 2.718281828,
 '2024-01-01', '2024-01-01 12:00:00', '{"key": "value", "number": 123}', 'option1', 'tag1,tag2',
 UNHEX('0123456789ABCDEF0123456789ABCDEF'), UNHEX('48656C6C6F20576F726C64'));

-- Create dsns table for pt-online-schema-change recursion method
CREATE TABLE dsns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    parent_id INT,
    dsn VARCHAR(255) NOT NULL,
    UNIQUE KEY dsn_key (dsn)
);

-- Insert DSN for recursion method
INSERT INTO dsns (dsn) VALUES ('h=mysql,P=3306,u=testuser,p=testpassword,D=testdb');

-- Show table information
SELECT
    TABLE_NAME,
    TABLE_ROWS,
    DATA_LENGTH,
    INDEX_LENGTH,
    (DATA_LENGTH + INDEX_LENGTH) as TOTAL_SIZE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'testdb'
ORDER BY TABLE_ROWS DESC;
