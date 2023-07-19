# - to activate conda environment: conda activate py38_env

import folium
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DoubleType
from streamlit_folium import st_folium

load_dotenv()

st.title("Data Engineering Jobs in the US")


# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()


session = create_session()
st.success("Connected to Snowflake!")


# Load data table
@st.cache_data
def load_data(table_name):
    ## Read in data table
    st.write(f"Here's some example data from `{table_name}`:")
    table = session.table(table_name)

    ## Do some computation on it
    table = table.limit(100)

    ## Collect the results. This will run the query and download the data
    table = table.collect()
    st.dataframe(table)


@st.cache_data
def load_map(table_name):
    st.write("Map of data engineering jobs in the US")
    lat_lon_df = (
        session.table(f"{table_name}")
        .select(col("LATITUDE"), col("LONGITUDE"))
        .filter(col("LATITUDE") != "unknown")
    ).to_pandas()

    lat_lon_df = lat_lon_df.astype({"LATITUDE": "float32", "LONGITUDE": "float32"})
    lat_lon_df.drop_duplicates()
    st.map(lat_lon_df)


def folium_map(table_name: str) -> None:
    st.header("Map of data engineering jobs in the US")
    lat_lon_df = (
        session.table(f"{table_name}")
        .select(col("CITY"), col("LATITUDE"), col("LONGITUDE"))
        .filter(col("LATITUDE") != "unknown")
    ).to_pandas()

    lat_lon_df = lat_lon_df.astype({"LATITUDE": "float32", "LONGITUDE": "float32"})
    lat_lon_df.drop_duplicates()

    lat_lon_tuples = list(
        zip(
            lat_lon_df["CITY"].to_list(),
            lat_lon_df["LATITUDE"].to_list(),
            lat_lon_df["LONGITUDE"].to_list(),
        )
    )

    m = folium.Map(location=[39.909726, -100.785153], zoom_start=4)
    for city, lat, long in lat_lon_tuples:
        folium.Marker([lat, long], popup=city, tooltip=city).add_to(m)

    # call to render Folium map in Streamlit
    st_data = st_folium(m, width=725)


def load_piechart_state(table_name: str) -> None:
    st.header("Top 10 US states with most data engineering jobs")
    sql_str = f"SELECT state,\
                COUNT(*) de_jobs_by_state\
                FROM {table_name}\
                WHERE state <> 'unknown'\
                GROUP BY state\
                ORDER BY COUNT(*) DESC\
                LIMIT 10;\
                "
    df = session.sql(sql_str).collect()
    fig = px.pie(df, values="DE_JOBS_BY_STATE", names="STATE")
    st.plotly_chart(fig)


def load_piechart_city(table_name: str) -> None:
    st.header("Top 10 US cities with most data engineering jobs")
    sql_str = f"SELECT city,\
                COUNT(*) de_jobs_by_city\
                FROM {table_name}\
                WHERE state <> 'unknown'\
                GROUP BY city\
                ORDER BY COUNT(*) DESC\
                LIMIT 10;\
                "
    df = session.sql(sql_str).collect()
    fig = px.pie(df, values="DE_JOBS_BY_CITY", names="CITY")
    st.plotly_chart(fig)


def main() -> None:
    table_name = "indeed_de_jobs_us"
    with st.sidebar:
        st.write("This is side bar")
    load_data(table_name)
    folium_map(table_name)
    load_piechart_state(table_name)
    load_piechart_city(table_name)


if __name__ == "__main__":
    main()
