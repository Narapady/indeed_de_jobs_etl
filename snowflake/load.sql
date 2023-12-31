-- create role and assign previlege to the created role
CREATE OR REPLACE ROLE indeed_role
   COMMENT = 'This role has all privileges on indeed_db and its schema';
GRANT USAGE
  ON WAREHOUSE indeed_wh
  TO ROLE indeed_role;

GRANT USAGE
  ON DATABASE indeed_db
  TO ROLE indeed_role;

GRANT USAGE
  ON SCHEMA indeed_db.indeed_schema
  TO ROLE indeed_role;

GRANT SELECT
  ON TABLE indeed_db.indeed_schema.indeed_de_jobs_us
  TO ROLE indeed_role;

GRANT CREATE STAGE 
    ON SCHEMA indeed_db.indeed_schema 
    TO ROLE indeed_role;
GRANT CREATE FILE FORMAT 
    ON SCHEMA indeed_db.indeed_schema 
    TO ROLE indeed_role;
GRANT USAGE 
    ON INTEGRATION s3_snowflake_integration 
    TO ROLE indeed_role;

USE DATABASE indeed_db; 
USE SCHEMA indeed_db.indeed_schema; 

CREATE OR REPLACE FILE FORMAT csv_format
    type = 'CSV'
    record_delimiter = '\n'
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';

-- create stage
CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_snowflake_integration
  URL = 's3://indeed-de-jobs/'
  FILE_FORMAT = csv_format;

-- copy data from s3 to snowflake table
COPY INTO indeed_de_jobs_us
  FROM @my_s3_stage;


















