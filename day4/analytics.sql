
show databases;
use banking_db.raw;
describe table raw_transactions;
use banking_db;
create schema transformed;
use schema transformed;

CREATE OR REPLACE TABLE BANKING_DB.TRANSFORMED.txn_cleaned AS
SELECT
    transaction_id,
    customer_id,
    account_id,
    amount,
    UPPER(TRIM(transaction_type)) AS transaction_type,
    transaction_ts,
    merchant,
    load_date
FROM BANKING_DB.RAW.raw_transactions2
WHERE amount IS NOT NULL
  AND transaction_ts IS NOT NULL;

select * from BANKING_DB.TRANSFORMED.txn_cleaned;

-- STEP 2 — DEDUPLICATION (CRITICAL FOR BANKING DATA)


create or replace table banking_db.analytics.tranformed_dedup 
as
select * 
from (select *,
            row_number() over(partition by transaction_id order by transaction_ts desc) as rn
            from BANKING_DB.TRANSFORMED.txn_cleaned
            ) t
            where rn = 1;


select * from banking_db.analytics.tranformed_dedup;


-- STEP 3 — CREATE ANALYTICS FACT TABLE


describe table banking_db.analytics.tranformed_dedup;

create or replace banking_db.analytics.fact_transactions
as
select transaction_id,
    customer_id,
    account_id,
    amount,
    transaction_type,
    transaction_ts,
    merchant,
    load_date
    from banking_db.analytics.tranformed_dedup;

CREATE OR REPLACE TABLE BANKING_DB.ANALYTICS.fact_transactions AS
SELECT
    transaction_id,
    customer_id,
    account_id,
    amount,
    transaction_type,
    transaction_ts,
    merchant,
    load_date
FROM banking_db.analytics.tranformed_dedup;

select * from banking_db.analytics.fact_transactions limit 5;

-- step 4: Business aggregations 
-- Daily customers spent

create or replace table BANKING_DB.ANALYTICS.daily_customers_spent as 
select customer_id, load_date, sum(amount) as Daily_Spent
from banking_db.analytics.fact_transactions
group by customer_id, load_date;

select * from BANKING_DB.ANALYTICS.daily_customers_spent;

-- another table

CREATE OR REPLACE TABLE BANKING_DB.ANALYTICS.daily_customer_spend2 AS
SELECT
    DATE(transaction_ts) AS txn_date,
    customer_id,
    SUM(amount) AS total_spend,
    count(*) as trans_count
FROM BANKING_DB.ANALYTICS.fact_transactions
GROUP BY 1, 2;

select * from BANKING_DB.ANALYTICS.daily_customer_spend2;


-- step_5 Basic data quality check
-- row count reconciliation

select 
(select count(*) from BANKING_DB.RAW.raw_transactions2) as raw_count,
(select count(*) from banking_db.analytics.tranformed_dedup) as dedup_count,
(select count(*) from BANKING_DB.ANALYTICS.fact_transactions) as fact_count;
