create database banking_db;
use banking_db;

-- create separate schemas
create schema raw;
create scheme transfomed;
create schema analytics;
use banking_db.raw;
# create file format

create or replace file format csv_format
type = csv,
field_delimiter = ',',
skip_header = 1,
null_if =('NULL', 'null'),
empty_field_as_null = True;

-- create  storage integration

CREATE OR REPLACE STORAGE INTEGRATION s3_integ
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = "arn:aws:iam::920822857299:role/aws-snowflake-role"
STORAGE_ALLOWED_LOCATIONS = ('s3://banking-data-jpmc/raw/transactions/');

 
desc storage integration s3_integ;

-- create s3 external storage stage

create or replace stage s3_stage
url = 's3://banking-data-jpmc/raw/transactions/'
storage_integration = s3_integ;

list@s3_stage;

CREATE OR REPLACE TABLE BANKING_DB.RAW.raw_transactions (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    amount NUMBER(10,2),
    transaction_type STRING,   
    transaction_ts TIMESTAMP,
    merchant STRING, 
    load_date DATE    -- derived column
);



copy into BANKING_DB.RAW.raw_transactions
from (SELECT $1 AS transaction_id,
            $2 AS customer_id,
            $3 AS account_id,
            $4 AS amount,
            $5 AS transaction_type,   
            $6 AS transaction_ts,
            $7 AS merchant,
            CURRENT_DATE AS load_date    -- derived column
            from @s3_stage
            )
FILE_FORMAT = CSV_FORMAT
on_error = 'continue'; 



-- select * from BANKING_DB.RAW.raw_transactions;
-- select * from BANKING_DB.RAW.load_audit;



-- store last query id so that if any query performed it will not change for audit load table

set copy_qid = last_query_id();

-- check metadata of copy into result, copy all column names for audit load table and create table

SELECT *
FROM TABLE(RESULT_SCAN($copy_qid));

--  file, status, rows_parsed,rows_loaded, error_limit, errors_seen, first_error_line, first_error_character, 
--  first_error_column_name 

-- create audit load table for above copy into result metadata storage

CREATE OR REPLACE TABLE BANKING_DB.RAW.audit_load (
    file STRING,
    status STRING,
    rows_parsed NUMBER,
    rows_loaded NUMBER,
    error_limit NUMBER,
    errors_seen NUMBER,
    first_error_line NUMBER,
    first_error_character NUMBER,
    first_error_column_name STRING
);



INSERT INTO BANKING_DB.RAW.audit_load
SELECT
    $1 as FILE,
    $2 as STATUS,
    $3 as ROWS_PARSED,
    $4 as ROWS_LOADED,
    $5 as ERROR_LIMIT,
    $6 as ERRORS_SEEN,
    $7 as FIRST_ERROR_LINE,
    $8 as FIRST_ERROR_CHARACTER,
    $9 as FIRST_ERROR_COLUMN_NAME
FROM TABLE(RESULT_SCAN($copy_qid));

SELECT * FROM BANKING_DB.RAW.audit_load;


-- # idempotent copy into

-- create another raw table for storing data in snowflake.

CREATE OR REPLACE TABLE BANKING_DB.RAW.raw_transactions1 (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    amount NUMBER(10,2),
    transaction_type STRING,   
    transaction_ts TIMESTAMP,
    merchant STRING, 
    load_date DATE    -- derived column
);


COPY INTO BANKING_DB.RAW.raw_transactions1
FROM (
    SELECT 
        $1 AS transaction_id,
        $2 AS customer_id,
        $3 AS account_id,
        $4 AS amount,
        $5 AS transaction_type,
        $6 AS transaction_ts,
        $7 AS merchant,
        CURRENT_DATE AS load_date        
    FROM @s3_stage
    )   
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE'
force = False;

set last_qid = last_query_id();

select * 
from table(result_scan($last_qid));


/*“Snowflake COPY INTO is inherently idempotent when force=false. In addition to that, I persist load metadata in an audit table to monitor ingestion, detect partial failures, and support controlled reprocessing, which is critical in financial data pipelines.” */

 
-- create another table for audit_load table with copyinto query result metadata storage for auditing.


CREATE OR REPLACE TABLE BANKING_DB.RAW.raw_transactions2 (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    amount NUMBER(10,2),
    transaction_type STRING,   
    transaction_ts TIMESTAMP,
    merchant STRING, 
    load_date DATE    -- derived column
);

COPY INTO BANKING_DB.RAW.raw_transactions2
FROM (
    SELECT 
        $1 AS transaction_id,
        $2 AS customer_id,
        $3 AS account_id,
        $4 AS amount,
        $5 AS transaction_type,
        $6 AS transaction_ts,
        $7 AS merchant,
        CURRENT_DATE AS load_date        
    FROM @s3_stage
    )   
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

set copy_last_qid = last_query_id();

select * from table(result_scan($copy_last_qid));

# create load audit table new one for dproduction level debugging

CREATE OR REPLACE TABLE BANKING_DB.RAW.audit_load1 (
    file STRING,
    status STRING,
    rows_parsed NUMBER,
    rows_loaded NUMBER,
    error_limit NUMBER,
    errors_seen NUMBER,
    first_error STRING,
    first_error_line NUMBER,
    first_error_character NUMBER,
    first_error_column_name STRING
);

-- insert data from copy into last result scan data
insert into BANKING_DB.RAW.audit_load1 
select * from table(result_scan($copy_last_qid));

select * from BANKING_DB.RAW.audit_load1;

-- add load_date and load_time columns to audit table

alter table BANKING_DB.RAW.audit_load1
add column load_date DATE;

alter table BANKING_DB.RAW.audit_load1
add column load_time TIMESTAMP;

-- insert meta data of copy into result_scan
truncate table BANKING_DB.RAW.audit_load1;
-- “Audit records include both technical metadata and operational timestamps.”

insert into BANKING_DB.RAW.audit_load1
select *, CURRENT_DATE as load_date, CURRENT_TIMESTAMP as load_time
from table(result_scan($copy_last_qid));

INSERT INTO BANKING_DB.RAW.audit_load1
SELECT 
    *,
    CURRENT_DATE AS load_date,
    CURRENT_TIMESTAMP AS load_time
FROM TABLE(RESULT_SCAN($copy_last_qid));

SELECT * FROM BANKING_DB.RAW.audit_load1;

ALTER TABLE BANKING_DB.RAW.audit_load1
ADD COLUMN LOAD_STATUS STRING;

UPDATE BANKING_DB.RAW.audit_load1
SET LOAD_STATUS =
                CASE WHEN errors_seen > 0 THEN 'PARTIALLY_SUCCESS'
                ELSE 'SUCCESS'
                END;



