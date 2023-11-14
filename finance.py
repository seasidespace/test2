import sys
import streamlit as st
import json
import pandas as pd
from snowflake import snowpark
from snowflake_exporter import export_to_snowflake
from snowflake_exporter import check_table_exists
from snowflake.snowpark import Session
from snowflake.connector import connect, DictCursor
from snowflake.connector import connect, ProgrammingError
from snowflake.connector.pandas_tools import write_pandas
import pyarrow.parquet as pq
from chatgptconnect import chatgpt_connect

st.write("""
# Data ingestion tool

Upload your dataset for processing!
""")

uploaded_file = st.file_uploader("Choose your file")
if uploaded_file is not None:
    table = pd.read_parquet(uploaded_file, engine='pyarrow')
    df = pd.DataFrame(table)
    st.write(table)



jsonfile = st.file_uploader("Upload your json file")
if jsonfile is not None:
    jsonData = json.loads(jsonfile.read().decode('utf-8'))
   # Create a DataFrame from the JSON data
    dataFrame = pd.DataFrame(jsonData)
    # Display the DataFrame
    st.write(dataFrame)

import re
def remove_special_characters(s):
    return re.sub(r'[^\d]', '', s)


if st.button('Apply data transform') and uploaded_file:
    transformed_df = df.copy()  # Create a copy to avoid modifying the original DataFrame

    for column, transformation in jsonData.items():
        if column in transformed_df.columns:
            try:
                if "astype" in transformation:
                    transformed_df[column] = transformed_df[column].astype(transformation["astype"])
                elif "map" in transformation:
                    transformed_df[column] = transformed_df[column].map(transformation["map"])
                elif "to_datetime" in transformation:
                    transformed_df[column] = pd.to_datetime(transformed_df[column], format=transformation["to_datetime"]["format"])
                elif "remove_special_characters" in transformation and transformation["remove_special_characters"]:
                    transformed_df[column] = transformed_df[column].apply(remove_special_characters)
                elif "remove_non_numeric" in transformation and transformation["remove_non_numeric"]:
                    transformed_df[column] = transformed_df[column].apply(remove_non_numeric)
                    transformed_df[column] = transformed_df[column].astype(int)
            except Exception as e:
                st.warning(f"Error transforming column {column}: {e}")
                print(f"Error transforming column {column}: {e}")
                print(transformed_df[column])  # Add this line to print the problematic value

        st.session_state['transformed_df'] = transformed_df

    st.write("Transformed DataFrame:")
    st.write(transformed_df.head())


# Get the file name from the user
file_name = st.text_input("Enter the file name (without extension):")


# Download button to save the transformed data as CSV
if st.button("Download DataFrame as CSV"):
    # Check if the DataFrame exists in session state
    if 'transformed_df' in st.session_state:
        # Get the DataFrame from session state
        transformed_df = st.session_state['transformed_df']

        # Check if the user entered a file name
        if file_name:
            # Save the DataFrame as CSV
            csv_content = transformed_df.to_csv(index=False)
            st.download_button(label="Download CSV", data=csv_content, file_name=f"{file_name}.csv", key='csv_button')
        else:
            st.warning("Please enter a file name before downloading.")
    else:
        st.warning("Please apply data transform before downloading.")

if 'transformed_df' in st.session_state:
        # Get the DataFrame from session state
        transformed_df = st.session_state['transformed_df']
        chatgpt_connect(transformed_df)


#  upload file to snowflake
st.write("Export Data to Snowflake SQL Database")

connection_params = {
    "user": st.secrets["connections"]["snowpark"]["user"],
    "password": st.secrets["connections"]["snowpark"]["password"],
    "account": st.secrets["connections"]["snowpark"]["account"],
    "warehouse": st.secrets["connections"]["snowpark"]["warehouse"],
    "database": st.secrets["connections"]["snowpark"]["database"],
    "schema": st.secrets["connections"]["snowpark"]["schema"],
    "role": st.secrets["connections"]["snowpark"]["role"],
}

@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.connections.snowpark).create()

session = create_session()
st.success("Connected to Snowflake!")

connection = connect(**connection_params)
cur = connection.cursor()
cur.execute("USE DATABASE FINANCE")
connection = connect(**connection_params)
cur = connection.cursor()
# cur.execute('create Table "TEST" ("customer Name" Varchar(30),"customer age" int)')



df_users = pd.DataFrame({
    "customer Name": ["John", "Alice"],
    "customer age": [30,15]
})

if check_table_exists(connection, "TEST"):
    # Use the write_pandas function to insert the DataFrame into the table
    write_pandas(connection, df_users, "TEST")

    st.success("Data successfully inserted into the Users table.")


# Dropdown box for user choice
action = st.selectbox("Choose action:", ["Create Table and Insert Data", "Insert Data into Existing Table"])

table_name = st.text_input("Enter the name of the table:")

if st.button("Export to Snowflake"):
    if not table_name:
        st.warning("Please enter the name of the table.")
    else:
        # with Session(**connection_params) as session:
        if action == "Create Table and Insert Data":
            export_to_snowflake(df, table_name, session, create_table=True)
        elif action == "Insert Data into Existing Table":
            export_to_snowflake(df, table_name, session, create_table=False)
