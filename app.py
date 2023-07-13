import os

import streamlit as st
from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv()

st.title("❄️ How to connect Streamlit to a Snowflake database")


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


table_name = "indeed_de_jobs_us"
with st.expander("See Table"):
    df = load_data(table_name)
    st.dataframe(df)
