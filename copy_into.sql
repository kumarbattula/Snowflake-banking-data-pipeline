# create audit table
  CREATE OR REPLACE TABLE BANKING_DB.RAW.load_audit (
    file_name STRING,
    load_date DATE,
    records_loaded NUMBER,
    load_status STRING,
    load_time TIMESTAMP,
    error_message STRING
);

# LOAD DATA USING COPY INTO (CORE INGESTION)
COPY INTO BANKING_DB.RAW.raw_transactions
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
    FROM @BANKING_DB.RAW.s3_stage
)
ON_ERROR = 'CONTINUE';


# DIRECT WAY
  COPY INTO BANKING_DB.RAW.raw_transactions
  FROM @BANKING_DB.RAW.s3_stage
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'CONTINUE'

# SAVE LAST_QUERY_ID() AS BELOW

copy_qid = last_query_id();

# CREATE AUDIT_LOAD TABLE FOR METADATA STORAGE
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

# INSERT METADATA INTO AUDIT_LOAD TABLE
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

