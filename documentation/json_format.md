# Ingestion : JSON file policy

## File naming scheme

To support incremental loading, files should be named with the **table name** and a **timestamp** value.

The timestamp value should be the most recent value in the "last_updated" field for each table being processed at the time of extraction.

All tables in the totesys database have the "last_updated" field.

E.g. `sales_order_2023-11-01 11:44:10.024.json`

## How files should be split

Each file should contain new or updated records since the previous file for a given table (or all records at time of execution if there are no previous files).

The "last_updated" field should be used to determine which records are new or updated.

For example if the first run were executed on 2023-11-01 at 10:15, the file for the sales_order table would be named:

`sales_order_2023-11-01 10:14:10.147.json`

This is because the most "recent" value of the "last_updated" field in this table at that time was "2023-11-01 10:14:10.147". It would contain 5017 records.

On the next run at, say, 10:20, a new file would be created, named:

`sales_order_2023-11-01 10:18:09.790.json`

At that time the most recent value of "last_updated" was "2023-11-01 10:18:09.790". It would contain 1 record.

On any given execution run, if no new or updated records are found with a "last_updated" timestamp which is newer than the timestamp of the previous file, a new file should **not** be created.

## File contents

Each json file should be structured as follows.

A single object with the following keys:

- **table_name** : name of the table this data is from.
- **column_names** : list of column names, in same order as the data.
- **record_count** : number of records in this file.
- **data** : Nested list. Each nested list item should contain the values for that record in the same order as the "column_names" above.

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
 "column_names" : ["sales_order_id","created_at","last_updated","design_id","staff_id","counterparty_id","units_sold","unit_price","currency_id","agreed_delivery_date","agreed_payment_date","agreed_delivery_location_id"],
 "record_count" : 3,
 "data" : [
  [5030, "2023-11-01 14:22:10.329","2023-11-01 14:22:10.329",186,11,17,51651,3.25,2,"2023-11-06","2023-11-05",27],
  [5029, "2023-11-01 14:12:10.124","2023-11-01 14:12:10.124",39,13,7,57395,3.49,1,"2023-11-02","2023-11-04",26],
  [5028, "2023-11-01 13:33:10.231","2023-11-01 13:33:10.231",229,20,13,34701,3.97,1,"2023-11-04","2023-11-03",28]
 ]
}
```

This file would be named `sales_order_2023-11-01 14:22:10.329.json` using the timestamp of the most recent value in the "last_updated" field.
