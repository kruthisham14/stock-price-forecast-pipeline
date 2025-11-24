from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.decorators import dag
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import yfinance as yf
import pandas as pd


def return_snowflake_conn():
    """Returns Snowflake cursor."""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        return conn.cursor()
    except Exception as e:
        print(f"Error connection to snowflake: {e}")
        raise


# 1. EXTRACT
@task
def extract_stock_prices():
    """Fetch stock prices from yfinance."""
    try:
        ticker_symbols = Variable.get("stock_ticker_list", default_var="AAPL,MSFT,GOOG").split(",")
        stock_data = yf.download(tickers=ticker_symbols, group_by="ticker", period="180d")
        return stock_data
    except Exception as e:
        print("Error fetching stock prices:", e)
        return pd.DataFrame()


# 2. TRANSFORM
@task
def transform_stock_data(stock_data):
    """Convert multi-index yfinance dataframe → clean table."""
    try:
        stacked_data = stock_data.stack(level=0, future_stack=True).reset_index()
        stacked_data.columns.name = None

        stacked_data = stacked_data[['Ticker', 'Date', 'Open', 'Close', 'Low', 'High', 'Volume']]
        stacked_data = stacked_data.rename(columns={'Ticker': 'StockSymbol', 'Low': 'Min', 'High': 'Max'})
        return stacked_data

    except Exception as e:
        print("Error transforming stock prices:", e)
        return pd.DataFrame()


# 3. INSERT helper
def insert_records(data, cursor, stock_table):
    """Insert row-by-row into Snowflake."""
    try:
        for idx, row in data.iterrows():
            insert_sql = f"""
                INSERT INTO {stock_table} (Symbol, Date, Open, Close, Min, Max, Volume)
                VALUES (
                    '{row.StockSymbol}',
                    '{row.Date}',
                    {row.Open if pd.notnull(row.Open) else 'NULL'},
                    {row.Close if pd.notnull(row.Close) else 'NULL'},
                    {row.Min if pd.notnull(row.Min) else 'NULL'},
                    {row.Max if pd.notnull(row.Max) else 'NULL'},
                    {row.Volume if pd.notnull(row.Volume) else 'NULL'}
                );
            """
            cursor.execute(insert_sql)

    except Exception as e:
        print("Error inserting records:", e)
        raise


# 4. LOAD
@task
def load_stock_prices_snowflake(data, stock_table):
    """Create table if needed → clear → load."""
    cursor = return_snowflake_conn()
    try:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {stock_table} (
                Symbol VARCHAR(5) NOT NULL,
                Date DATE NOT NULL,
                Open FLOAT NOT NULL,
                Close FLOAT NOT NULL,
                Min FLOAT NOT NULL,
                Max FLOAT NOT NULL,
                Volume FLOAT NOT NULL,
                PRIMARY KEY (Symbol, Date)
            );
        """

        cursor.execute("BEGIN;")
        cursor.execute(create_table_sql)

        # CLEAR TABLE
        cursor.execute(f"DELETE FROM {stock_table}")

        # INSERT NEW
        insert_records(data, cursor, stock_table)

        cursor.execute("COMMIT;")
        return "Loaded into Snowflake"

    except Exception as e:
        print("Error during load:", e)
        cursor.execute("ROLLBACK;")
        raise


# DAG DEFINITION
@dag(
    dag_id="daily_yfinance_stock_ingestion",
    start_date=datetime(2025, 10, 4),
    catchup=False,
    schedule_interval="0 0 * * *",
    tags=["ETL"]
)
def etl_pipeline():

    stock_table = Variable.get("stock_table_name")

    stock_data = extract_stock_prices()
    transformed_data = transform_stock_data(stock_data)
    loaded = load_stock_prices_snowflake(transformed_data, stock_table)

    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_dag",
        trigger_dag_id="daily_stock_price_forecasting",
        wait_for_completion=True,
        poke_interval=60,
    )

    loaded >> trigger_forecast


etl_pipeline()
