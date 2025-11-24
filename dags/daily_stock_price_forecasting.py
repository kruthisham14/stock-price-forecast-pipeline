from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def return_snowflake_conn():
    """Returns a Snowflake cursor using Airflow connection."""
    try:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        return conn.cursor()
    except Exception as e:
        print("Error connecting to snowflake", e)
        raise

# -------------------------------
# TASK 1 — TRAIN MODEL
# -------------------------------
@task
def train(cursor, train_input_table, train_view, forecast_function_name):
    """Creates training view and trains Snowflake ML Forecast model."""

    # 1 — Create modeling view
    create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT 
            TO_TIMESTAMP_NTZ(DATE) AS DATE_V1,
            CLOSE,
            SYMBOL
        FROM {train_input_table}
        WHERE CLOSE IS NOT NULL;
    """

    # 2 — Train model
    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name}(
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE_V1',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );
    """

    try:
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)

        # Optional: show evaluation metrics
        cursor.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")

        print("Model training completed successfully.")

    except Exception as e:
        print("Error during training:", e)
        raise


# -------------------------------
# TASK 2 — PREDICT
# -------------------------------
@task
def predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table):
    """Generates 7-day forecast and builds final union table."""

    # 1 — Generate predictions
    make_prediction_sql = f"""
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{ 'prediction_interval': 0.95 }}
        );
    """

    # 2 — Save forecast results into table
    store_forecast_sql = f"""
        CREATE OR REPLACE TABLE {forecast_table} AS
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    """

    # 3 — Combine historical + forecast
    create_final_sql = f"""
        CREATE OR REPLACE TABLE {final_table} AS
        SELECT
            SYMBOL,
            DATE AS PRICE_DATE,
            CLOSE AS ACTUAL,
            NULL AS FORECAST,
            NULL AS LOWER_BOUND,
            NULL AS UPPER_BOUND
        FROM {train_input_table}

        UNION ALL

        SELECT
            REPLACE(SERIES, '"', '') AS SYMBOL,
            TS AS PRICE_DATE,
            NULL AS ACTUAL,
            FORECAST,
            LOWER_BOUND,
            UPPER_BOUND
        FROM {forecast_table};
    """

    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(store_forecast_sql)
        cursor.execute(create_final_sql)

        print("Forecasts generated and final table created.")

    except Exception as e:
        print("Error during forecasting:", e)
        raise


# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="daily_stock_price_forecasting",
    start_date=datetime(2025, 10, 4),
    catchup=False,
    schedule_interval= "45 0 * * *",   # Triggered by ETL DAG
    tags=["ML", "FORECAST"]
) as dag:

    # Airflow Variables
    train_input_table = Variable.get("stock_table_name")            # RAW.YFINANCE_STOCK
    train_view = Variable.get("train_view_name")                    # RAW.STOCK_DATA_VIEW
    forecast_table = Variable.get("forecast_table_name")            # ANALYTICS.STOCK_FORECAST_7D
    forecast_function_name = Variable.get("forecast_function_name") # ANALYTICS.STOCK_MODEL
    final_table = Variable.get("final_table_name")                  # ANALYTICS.STOCK_ANALYTICS_FINAL

    cursor = return_snowflake_conn()

    train_task = train(cursor, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table)

    train_task >> predict_task
