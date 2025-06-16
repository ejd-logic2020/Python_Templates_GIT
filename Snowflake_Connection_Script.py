import os
import logging
import snowflake.connector
import pandas as pd
from typing import Optional

# --------------------------------------------
# Configuration Section
# --------------------------------------------
CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER", "your_username"),
    "role": os.getenv("SNOWFLAKE_ROLE", "your_role"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "your_account.region.cloud"),
    "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),  # Or 'snowflake' for password
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "your_warehouse"), # Optional
    "database": os.getenv("SNOWFLAKE_DATABASE", "your_database"), # Optional
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "your_schema"), # Optional
    "proxy": os.getenv("HTTPS_PROXY", ""),  # Optional
}

# --------------------------------------------
# Logging Setup
# --------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# --------------------------------------------
# Connection Function
# --------------------------------------------
def get_snowflake_connection(config: dict = CONFIG) -> Optional[snowflake.connector.SnowflakeConnection]:
    """
    Establish and return a Snowflake connection using the provided configuration.
    """
    try:
        conn_params = {
            "user": config["user"],
            "account": config["account"],
            "role": config["role"],
            "authenticator": config["authenticator"],
            "warehouse": config["warehouse"],
            "database": config["database"],
            "schema": config["schema"]
        }
        if config.get("proxy"):
            os.environ["HTTPS_PROXY"] = config["proxy"]

        conn = snowflake.connector.connect(**conn_params)
        logging.info("Snowflake connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        return None

# --------------------------------------------
# Query Execution Function
# --------------------------------------------
def run_query(query: str, conn: snowflake.connector.SnowflakeConnection) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a pandas DataFrame.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            df = cur.fetch_pandas_all()
        logging.info("Query executed successfully.")
        return df
    except Exception as e:
        logging.error(f"Query failed: {e}")
        return pd.DataFrame()



# --------------------------------------------
# Example Usage
# --------------------------------------------
 
# Example Query, need to personalize
SQL_query = f"""
  -- Combine the two datasets by stacking them on top of each other
  SELECT 
    kwh_in,
    kwh_out,
    net_kwh,
    date,
    customer_class,
    city
  FROM
    usage_data A
  JOIN 
    customer_info B
    ON A.customer_id = B.customer_id
  ORDER BY
    date 
"""
conn = get_snowflake_connection()
if conn:
    test_query = SQL_query # "Can personalize query inside or outside of this line..."
    result_df = run_query(test_query, conn)
    print(result_df)
    conn.close()


# --------------------------------------------
# Python Operations
# --------------------------------------------

# From here, you can run any sort of python operations on the data. Below are some useful examples.

# Renamne
df_renamed = df.rename(columns={'orig1': 'new1', 'orig2': 'new2'}) # orig : New

# Drop / Subset
df_dropped = df.drop(columns=['column1'])
df[['column1', 'column2']]

# Filter
df_filtered = df[df['column1'] > 10]

# Group by -> summarize
grouped = df.groupby('Region').agg(total1 = ('column1', 'sum'), avg2 = ('column2', 'mean')).reset_index()

# Sorted
df_sorted = df.sort_values(by='column1', ascending=False)

# Apply custom function
df['Col1_Label'] = df['column1'].apply(lambda x: 'High' if x > 200 else 'Low')

# Merge
merged_df = pd.merge(df, df2, on='Merge_Col', how='left')

# Pivot (similar to group by -> summarize)
pivot = df.pivot_table(index='column1', values='column2', aggfunc='sum')

# Date operations
df['OrderDate'] = pd.to_datetime(df['OrderDate'])
df['Year'] = df['OrderDate'].dt.year
df['Month'] = df['OrderDate'].dt.month
df['Weekday'] = df['OrderDate'].dt.day_name()

# Missing Data
df_filled = df.fillna(value={'Price': 0, 'Stock': df['Stock'].mean()})
df.ffill() # forward fill

# Can re-shape using melt, pivot