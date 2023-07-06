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
    con.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    con.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    con.execute(f"USE DATABASE {database_name}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    con.execute(f"USE WAREHOUSE {warehouse_name}")
    con.execute(f"USE DATABASE {database_name}")
    con.execute(f"USE SCHEMA {database_name}.{schema_name}")

    df = pd.read_csv("./dataset/indeed_de_jobs_clean.csv")
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {talbe_name} (\n"

    for idx, col in enumerate(df.columns):
        if df.columns[idx] == df.columns[-1]:
            create_table_sql = create_table_sql + " " + col + " varchar(1000)" + "\n"
            create_table_sql = create_table_sql + ")"
        else:
            create_table_sql = create_table_sql + " " + col + " varchar(1000)," + "\n"

    # - file format

    con.execute(create_table_sql)


def load_target_table(
    con: SnowflakeConnection, tablename: str, csv_filepath: str
) -> None:
    # df = pd.read_csv("./dataset/indeed_de_jobs.clean")
    # con.execute(
    #     f"INSERT INTO {tablename}(col1, col2) VALUES "
    #     + "    (123, 'test string1'), "
    #     + "    (456, 'test string2')"
    # )
    # s = f"INSERT INTO test_table({[col for col in df.columns]}) VALUES "
    # s = s.replace("[", "").replace("]", "").replace("'", "")
    # Putting Data
    con.execute(f"PUT file://{csv_filepath} @%{tablename}")

    # file_format = """
    #     create or replace file format csv_format
    #     type = 'CSV'
    #     record_delimiter = "\n"
    #     field_delimiter = ','
    #     skip_header = 1
    #     empty_field_as_null = true
    #     FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
    # """
    con.execute(
        f"COPY INTO {tablename} FILE_FORMAT = (TYPE = 'csv' RECORD_DELIMITER = '\\n' SKIP_HEADER = 1) ON_ERROR='SKIP_FILE_5"
    )


def main() -> None:
    filepath = Path.cwd() / "dataset" / "indeed_de_jobs_clean.csv"
    con = connect_snowflake()
    create_target_table(con, WAREHOUSE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
    load_target_table(con, TABLE_NAME, filepath)
    con.close()


if __name__ == "__main__":
    main()
