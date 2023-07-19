# - to activate conda environment: conda activate py38_env

import folium
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
    return table


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
    st.write("Map of data engineering jobs in the US")
    lat_lon_df = (
        session.table(f"{table_name}")
        .select(col("CITY"), col("LATITUDE"), col("LONGITUDE"))
        .filter(col("LATITUDE") != "unknown")
    ).to_pandas()

    lat_lon_df = lat_lon_df.astype({"LATITUDE": "float32", "LONGITUDE": "float32"})
    lat_lon_df.drop_duplicates()

    # center on Liberty Bell, add marker
    lat_lon_tuples = list(
        zip(
            lat_lon_df["CITY"].to_list(),
            lat_lon_df["LATITUDE"].to_list(),
            lat_lon_df["LONGITUDE"].to_list(),
        )
    )

    m = folium.Map(location=[39.949610, -75.150282], zoom_start=5)
    for city, lat, long in lat_lon_tuples:
        folium.Marker([lat, long], popup=city, tooltip=city).add_to(m)

    # call to render Folium map in Streamlit
    st_data = st_folium(m, width=725)


def main() -> None:
    table_name = "indeed_de_jobs_us"
    with st.expander("See Table"):
        df = load_data(table_name)
        st.dataframe(df)

    folium_map(table_name)


if __name__ == "__main__":
    main()
