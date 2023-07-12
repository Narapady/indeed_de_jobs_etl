USE ROLE ACCOUNTADMIN;
SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();

-- create storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_snowflake_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::209055978788:role/s3-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://indeed-de-jobs/', 's3://indeed-de-jobs/');

-- verify the stroage integration
DESC INTEGRATION s3_snowflake_integration;