import folium
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from snowflake.snowpark import Session, Table
from snowflake.snowpark.functions import col
from streamlit_folium import st_folium

load_dotenv()


TABLE_NAME = "indeed_de_jobs_us"


# Establish Snowflake session
@st.cache_resource
def create_session() -> Session:
    return Session.builder.configs(st.secrets.snowflake).create()


def load_table(session: Session, table_name: str) -> Table:
    return session.table(table_name)


# Load data table
def load_dataframe(table: Table) -> None:
    st.write(f"Dataframe from `{TABLE_NAME}`:")
    df = table.collect()
    st.dataframe(df)


def load_map(table: Table) -> None:
    st.header("Map of data engineering jobs in the US")
    lat_lon_df = (
        table.select(col("LATITUDE"), col("LONGITUDE")).filter(
            col("LATITUDE") != "unknown"
        )
    ).to_pandas()

    lat_lon_df = lat_lon_df.astype({"LATITUDE": "float32", "LONGITUDE": "float32"})
    lat_lon_df.drop_duplicates()
    st.map(lat_lon_df, zoom=2)


def folium_map(table: Table) -> None:
    st.header("Folium map of data engineering jobs in the US")
    lat_lon_df = (
        table.select(col("CITY"), col("LATITUDE"), col("LONGITUDE")).filter(
            col("LATITUDE") != "unknown"
        )
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


def load_piechart(session: Session, table_name: str, level: str) -> None:
    st.header(f"Top 10 US {level} with most data engineering jobs")
    sql_str = f"SELECT {level},\
                COUNT(*) de_jobs_by_{level}\
                FROM {table_name}\
                WHERE {level} <> 'unknown'\
                GROUP BY {level}\
                ORDER BY COUNT(*) DESC\
                LIMIT 10;\
                "
    df = session.sql(sql_str).collect()
    fig = px.pie(df, values=f"DE_JOBS_BY_{level.upper()}", names=level.upper())
    st.plotly_chart(fig)


def load_barchat_worktype(session: Session, table_name: str) -> None:
    sql_str = f" SELECT work_type,\
                    work_hour,\
                    count(*) AS work_type_cnt\
                FROM {table_name}\
                GROUP BY work_type, work_hour\
                ORDER BY count(*) DESC;\
                "
    st.header("Number of work type in data engineering jobs")
    df = session.sql(sql_str).collect()
    fig = px.bar(df, x="WORK_TYPE", y="WORK_TYPE_CNT", color="WORK_HOUR")
    st.plotly_chart(fig)


def load_barchart_jobs_by_company(session: Session, table_name: str) -> None:
    st.header("Companies that offer more than 1 data engineering jobs")
    sql_str = f"SELECT company_name,\
                COUNT(*) num_jobs_by_company\
            FROM {table_name}\
            GROUP BY company_name\
            HAVING COUNT(*) > 1\
            ORDER BY COUNT(*);\
            "
    df = session.sql(sql_str).collect()
    fig = px.bar(
        df, x="NUM_JOBS_BY_COMPANY", y="COMPANY_NAME", color="NUM_JOBS_BY_COMPANY"
    )
    st.plotly_chart(fig)


def load_sidebar() -> None:
    with st.sidebar:
        st.header("979 Data enginering jobs data is scrapped from indeed.com")
        st.write(
            "Data is ingestd to Amazon S3, transformed, then loaded to Snowflake.\
             Streamlit App loads data fram Snowflake using Snowpark API and displays insights"
        )
        st.divider()

        st.title("Tools and libaries used for the project")
        st.markdown("- Scrap and transform data: Python")
        st.markdown("- Create table and load data to Snowflake: SQL")
        st.markdown("- Web Scrapping: BeautifulSoup, requests")
        st.markdown("- S3 API: Amazon Boto3")
        st.markdown("- Snowflake API: Snowflake connector, Snowpark")
        st.markdown("- Data Manipulation: Pandas, Snowpark")
        st.markdown("- Visualization: Streamlit, Plotly")


def main() -> None:
    st.title("Data Engineering Jobs in the US")
    load_sidebar()

    session = create_session()
    if session:
        st.success("Connected to Snowflake!")

    indeed_job_tbl = load_table(session, TABLE_NAME)
    load_dataframe(table=indeed_job_tbl)
    load_map(table=indeed_job_tbl)
    folium_map(table=indeed_job_tbl)
    load_piechart(session, TABLE_NAME, "state")
    load_piechart(session, TABLE_NAME, "city")
    load_barchat_worktype(session, TABLE_NAME)
    load_barchart_jobs_by_company(session, TABLE_NAME)


if __name__ == "__main__":
    main()
