# Data relationships

The below is a brief analysis between the OLTP and OLAP schemas to facilitate design of the required functionality.

## OLAP db requirements

The following tables can be populated simply by copying the required fields from a single table without any transformation:

- **fact_purchase_order** : purchase_order
- **fact_sales_order** : sales_order
- **fact_payment** : payment
- **dim_location** : address
- **dim_payment_type** : payment_type
- **dim_transaction** : transaction
- **dim_design** : design

The following require multiple tables to compose each record:

- **dim_counterparty** : counterparty, address
- **dim_staff** : staff, department

The following require special attention to populate properly:

- **dim_currency** : currency, **name** is not in oltp data. Appears to be currency codes, could be pre-populated.
- **dim_date** : a strategy for pre-populating data may be required

## Data Lake (parquet)

To support the OLAP db, the data stored as parquet files should be transformed in a state ready to load into the database.

Because of this, files should be organised as such e.g. fact_purchase_order.parquet, fact_sales_order.parquet, dim_location.parquet etc.

## Other considerations

Each fact table in the OLAP database has its own primary key independent of the key of the original OLTP database. This facilitates change capture.
