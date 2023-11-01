# Ingestion : JSON file policy

## File naming scheme

To support incremental loading, files should be named with the **table name** and a **timestamp** value.

The timestamp value should be the most recent value in the "last_updated" field for each table being processed at the time of extraction.

All tables in the totesys database have the "last_update" field.

E.g. `sales_order_2023-11-01 11:44:10.024.json`

## How files should be split

Each file should contain all records since the previous file for a given table (or all records at time of execution if there are no previous files).

For example if the first run were executed on 2023-11-01 at 10:15, the file for the sales_order table would be named:

`sales_order_2023-11-01 10:14:10.147.json`

This is because the most "recent" value of the "last_updated" field in this table at that time was "2023-11-01 10:14:10.147". It would contain 5017 records.

On the next run at, say, 10:20, the file would be named:

`sales_order_2023-11-01 10:18:09.790.json`

At that time the most recent value of "last_updated" was "2023-11-01 10:18:09.790". It would contain 1 record.

If no records are found with a "last_updated" timestamp after the timestamp of the previous file, a new file should **not** be created.

## File contents

Each json file should conform to the following structure.

An object with the following keys:

- table_name : name of this table.
- column_names : list of column names, in same order as the data.
- record_count : number of records in this file.
- data : list of lists. Each nested list should contain the values for that record in the same order as the "column_names" above.

```json
{
 "table_name" : "table",
 "column_names" : ["field1", "field2" ... ],
 "record_count" : 1,
 "data" : [[1, "a"], [2, "b"], ... ]
}
```

Sample contents:

```json
{
 "table_name" : "sales_order",
 "column_names" : ["sales_order_id", "created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_i"],
 "record_count" : 3,
 "data" : [
  ['2022-11-03 14:20:52.186','2022-11-03 14:20:52.186',9,16,18,84754,2.43,3,'2022-11-10','2022-11-03',4],
  ['2023-11-01 11:44:10.024','2023-11-01 11:44:10.024',81,16,14,47564,3.28,1,'2023-11-04','2023-11-02',2],
  ['2023-11-01 10:56:10.107','2023-11-01 10:56:10.107',167,16,10,28458,3.42,3,'2023-11-05','2023-11-03',8]
 ]
}
```
