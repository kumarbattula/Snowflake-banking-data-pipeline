# create a external stage integration object

CREATE OR REPLACE STORAGE INTEGRATION s3_integ
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = "arn:aws:iam::920822857299:role/aws-snowflake-role"
STORAGE_ALLOWED_LOCATIONS = ('s3://banking-data-jpmc/raw/transactions/');

# create s3_stage

create or replace stage s3_stage
url = 's3://banking-data-jpmc/raw/transactions/'
storage_integration = s3_integ;

