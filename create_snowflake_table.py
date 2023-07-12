import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection

load_dotenv()

WAREHOUSE_NAME = "indeed_wh"
DATABASE_NAME = "indeed_db"
SCHEMA_NAME = "indeed_schema"
TABLE_NAME = "indeed_de_jobs_us"


def connect_snowflake() -> SnowflakeConnection:
    cnn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ORG_NAME") + "-" + os.getenv("SNOWFLAKE_ACC_NAME"),
    )

    return cnn.cursor()


def create_target_table(
    con: SnowflakeConnection,
    warehouse_name: str,
    database_name: str,
    schema_name: str,
    talbe_name: str,
) -> None:
    con.execute(f"CREATE OR REPLACE WAREHOUSE {warehouse_name}")
    con.execute(f"USE WAREHOUSE {warehouse_name}")
    con.execute(f"CREATE OR REPLACE DATABASE {database_name}")
    con.execute(f"USE DATABASE {database_name}")
    con.execute(f"CREATE OR REPLACE SCHEMA {schema_name}")
    con.execute(f"USE SCHEMA {database_name}.{schema_name}")

    df = pd.read_csv("./dataset/indeed_de_jobs_cleaned.csv")
    create_table_sql = f"CREATE OR REPLACE TABLE  {talbe_name} (\n"

    for idx, col in enumerate(df.columns):
        if df.columns[idx] == df.columns[-1]:
            create_table_sql = create_table_sql + " " + col + " varchar(1000)" + "\n"
            create_table_sql = create_table_sql + ")"
        else:
            create_table_sql = create_table_sql + " " + col + " varchar(1000)," + "\n"

    con.execute(create_table_sql)


def load_target_table(
    con: SnowflakeConnection, tablename: str, csv_filepath: str
) -> None:
    # Putting Data
    con.execute(f"PUT file://{csv_filepath} @%{tablename}")

    delimeter = r"'\n'"
    csv_format = (
        "create or replace file format csv_format\n"
        + " type = 'CSV'\n"
        + f" record_delimiter ={delimeter} \n"
        + " field_delimiter = ','\n"
        + " skip_header = 1\n"
        + " empty_field_as_null = true\n"
        + " FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'\n"
    )
    con.execute(csv_format)

    con.execute(f"COPY INTO {tablename} FILE_FORMAT = 'csv_format'")


def main() -> None:
    filepath = Path.cwd() / "dataset" / "indeed_de_jobs_cleaned.csv"
    con = connect_snowflake()
    create_target_table(con, WAREHOUSE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
    # load_target_table(con, TABLE_NAME, filepath)
    con.close()


if __name__ == "__main__":
    main()
