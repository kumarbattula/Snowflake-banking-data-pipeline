-- step-1: CREATE LOAD_AUDIT TABLE 

CREATE OR REPLACE TABLE load_audit_idem
as select * from audit_load1
where 1 = 0;

describe table load_audit_idem;  -- returns table schema

list @s3_stage;   -- show all files

-- step_2 : Identify files that should be loaded from s3_stage, create view to find files that are not loaded or filed


CREATE OR REPLACE VIEW files_to_load AS
SELECT
    METADATA$FILENAME AS file_name
FROM @s3_stage
WHERE METADATA$FILENAME NOT IN (
    SELECT file
    FROM BANKING_DB.RAW.load_audit_idem
    WHERE load_status = 'SUCCESS'
);

select * from files_to_load;
 
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
    WHERE METADATA$FILENAME NOT IN (
        SELECT file
        FROM BANKING_DB.RAW.load_audit_idem
        WHERE load_status = 'SUCCESS'
    )
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';
