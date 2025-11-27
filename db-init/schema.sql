-- Create ENUM type for result
CREATE TYPE result_enum AS ENUM ('0','1');

-- Create results table
CREATE TABLE IF NOT EXISTS results (
    id            SERIAL PRIMARY KEY,
    email_address VARCHAR(255),
    result        result_enum NOT NULL,
    additional_info TEXT,
    checked_by    VARCHAR(255),
    checked_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(255) PRIMARY KEY,
    "key"    VARCHAR(255),
    credits  INT DEFAULT 0,
    label    VARCHAR(255)
);
