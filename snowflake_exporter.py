import pandas as pd
from snowflake.connector import connect, ProgrammingError
from snowflake.connector.pandas_tools import write_pandas
import streamlit as st


def check_table_exists(connection, table_name):
    try:
        # This function checks if the table already exists in Snowflake
        query = f"SHOW TABLES LIKE '{table_name.upper()}'"
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchone() is not None
    except ProgrammingError as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        cursor.close()


def create_table(connection, table_name, df):
    # Generate a CREATE TABLE statement based on the DataFrame structure
    create_table_statement = f"CREATE OR REPLACE TABLE {table_name} ({', '.join([f'{col} STRING' for col in df.columns])})"
    connection.cursor().execute(create_table_statement)


def write_to_table(connection, table_name, df):
    try:
        cursor = connection.cursor()

        # Check if the table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            # Retrieve table columns
            cursor.execute(f"DESC TABLE {table_name.upper()}")
            table_description = cursor.fetchall()
            table_columns = [desc[0] for desc in table_description]

            # Check if DataFrame columns match the existing table columns
            df_columns = df.columns.tolist()

            if set(df_columns).issubset(set(table_columns)):
                # If columns match, insert data into the existing table
                write_pandas(connection, df, table_name.upper())
                st.success(f"Successfully inserted data into Table: `{table_name}`")
            else:
                st.warning('The columns of the DataFrame do not match the columns of the existing table.')
        else:
            st.warning(f"The table `{table_name}` does not exist.")
    except Exception as e:
        st.error(f"An error occurred while creating the table: {e}")
    finally:
        cursor.close()


def export_to_snowflake(df, table_name, connection_params, create_table=True):
    connection = connect(**connection_params)

    if create_table:
        if not check_table_exists(connection, table_name):
            create_table(connection, table_name, df)
            st.success(f"Table `{table_name}` created successfully.")
        else:
            st.warning(f"The table `{table_name}` already exists.")

    # Insert data into the table
    write_to_table(connection, table_name, df)

    st.success("Data successfully exported to Snowflake!")
