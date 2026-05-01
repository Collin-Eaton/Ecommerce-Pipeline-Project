# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 08:56:57 2026

@author: colli
"""

import requests
import snowflake.connector
import pandas as pd
import datetime
import io
import os
import logging

# ------------------------
# LOGGING SETUP
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------------------
# CONFIG
# ------------------------

BASE_URL = "http://localhost:8000"

SNOWFLAKE_CONFIG = {
    "user": "COLLINWILLIAMEATON777",
    "password": "iRb9veDFg2kpihF",
    "account": "MKQWGLA-JYB47897",
    "warehouse": "ecommerce",
    "database": "ecommerce_db",
    "schema": "ecommerce_schema"
}


# ------------------------
# EXTRACT
# ------------------------

def fetch_data(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch {endpoint}: {response.text}")

    return response.json()

# ------------------------
# TRANSFORM
# ------------------------
def transform_to_df(data):
    df = pd.DataFrame(data)
    return df

# ------------------------
# GET MAX ID FROM SNOWFLAKE
# ------------------------
def get_max_id(conn, table_name, id_column):
    cursor = conn.cursor()

    try:
        query = f"SELECT MAX({id_column}) FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchone()[0]

        return result if result is not None else -1
    except:
        # Table might not exist yet
        return -1
    finally:
        cursor.close()


# ------------------------
# LOAD
# ------------------------
def load_to_snowflake(df, table_name):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # Create table if not exists (basic version)
    columns = ", ".join([f"{col} STRING" for col in df.columns])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({columns});
    """
    cursor.execute(create_table_sql)

    # Insert data
    for _, row in df.iterrows():
        values = ", ".join([f"'{str(val)}'" for val in row.values])
        insert_sql = f"INSERT INTO {table_name} VALUES ({values});"
        cursor.execute(insert_sql)

    conn.commit()
    cursor.close()
    conn.close()
    
    
# ------------------------
# MAIN PIPELINE
# ------------------------
def run_pipeline():
    endpoints = ["customers", "orders", "products", "returns"]

    for endpoint in endpoints:
        print(f"Processing {endpoint}...")

        data = fetch_data(endpoint)
        df = transform_to_df(data)

        table_name = f"raw_{endpoint}"
        load_to_snowflake(df, table_name)

        print(f"Loaded {len(df)} rows into {table_name}")

if __name__ == "__main__":
    run_pipeline()
    
    

