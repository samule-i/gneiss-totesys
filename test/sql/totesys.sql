DROP DATABASE IF EXISTS test_database;

CREATE DATABASE test_database;

\c test_database;

CREATE TABLE sales_order (
    sales_order_id INT PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    design_id INT,
    staff_id INT,
    counterparty_id INT,
    units_sold INT,
    unit_price NUMERIC(10,2),
    currency_id INT,
    agreed_delivery_date DATE,
    agreed_payment_date DATE,
    agreed_delivery_location_id INT
);

CREATE TABLE address (
    address_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);
INSERT INTO address (address_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE counterparty (
    counterparty_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);

CREATE TABLE currency (
    currency_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);
INSERT INTO currency (currency_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE department (
    department_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);
INSERT INTO department (department_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE design (
    design_id INT PRIMARY KEY,
    last_updated TIMESTAMP(3)
);
INSERT INTO design (design_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE payment (
    payment_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);
INSERT INTO payment (payment_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE payment_type (
    payment_type_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);
INSERT INTO payment_type (payment_type_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE purchase_order (
    purchase_order_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);
INSERT INTO purchase_order (purchase_order_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE staff (
    staff_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);

INSERT INTO staff (staff_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");

CREATE TABLE transaction (
    transaction_id INT PRIMARY KEY,
    last_updated TIMESTAMP
);

INSERT INTO transaction (transaction_id, last_updated) VALUES (100, "2023-11-06 10:00:00.000");


INSERT INTO sales_order (
    sales_order_id, created_at, last_updated, design_id, staff_id, counterparty_id,
    units_sold, unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id
) VALUES
    (5030, '2023-11-01 14:22:10.329', '2023-11-01 14:22:10.329', 186, 11, 17, 51651, 3.25, 2, '2023-11-06', '2023-11-05', 27),
    (5029, '2023-11-01 14:12:10.124', '2023-11-01 14:12:10.124', 39, 13, 7, 57395, 3.49, 1, '2023-11-02', '2023-11-04', 26),
    (5028, '2023-11-01 13:33:10.231', '2023-11-01 13:33:10.231', 229, 20, 13, 34701, 3.97, 1, '2023-11-04', '2023-11-03', 28);