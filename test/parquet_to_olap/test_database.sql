CREATE DATABASE parquet_to_sql_test;

\c parquet_to_sql_test;

DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location (
	location_id int4 NOT NULL,
	address_line_1 varchar NOT NULL,
	address_line_2 varchar NULL,
	district varchar NULL,
	city varchar NOT NULL,
	postal_code varchar NOT NULL,
	country varchar NOT NULL,
	phone varchar NOT NULL,
	CONSTRAINT dim_location_pkey PRIMARY KEY (location_id)
);


