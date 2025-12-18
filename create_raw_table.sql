CREATE OR REPLACE TABLE BANKING_DB.RAW.raw_transactions (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    amount NUMBER(10,2),
    transaction_type STRING,   
    transaction_ts TIMESTAMP,
    merchant STRING,
    load_date DATE
);
