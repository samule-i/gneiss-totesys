CREATE DATABASE test_database;

\c test_database;

CREATE TABLE test_table (
    id serial PRIMARY KEY,
    column1 VARCHAR(255),
    column2 INTEGER,
    column3 BOOLEAN
);

INSERT INTO test_table (column1, column2, column3)
VALUES
    ('Value 1', 42, TRUE),
    ('Value 2', 123, FALSE),
    ('Value 3', 987, TRUE);

CREATE TABLE empty_table (
    id serial PRIMARY KEY
);

CREATE DATABASE empty_test_database;
