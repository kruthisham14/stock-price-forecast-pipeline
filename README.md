📈 Stock Price Prediction Pipeline (Airflow + Snowflake + YFinance)

This project implements a complete data engineering + machine learning forecasting pipeline using Apache Airflow, Snowflake, and YFinance.
The pipeline automatically ingests daily stock data, stores it in Snowflake, trains a Snowflake ML forecasting model, and generates 7-day future stock price predictions.


🚀 Features
✔ Automated Stock Data Ingestion

Pulls last 180 days of historical stock data using yfinance.

Supports multiple tickers: AAPL, MSFT, GOOG (configurable via Airflow Variables).

Cleans, transforms, and loads the data into RAW.YFINANCE_STOCK Snowflake table.

✔ Snowflake ML Forecast Model

Creates a training view with timestamp + target column.

Builds a Snowflake native ML forecasting model:

SNOWFLAKE.ML.FORECAST ANALYTICS.PREDICT_STOCK_PRICE


Generates 7-day forward predictions for each stock.

✔ Unified Final Analytics Table

Produces a combined table containing both historical actuals and future predicted values:

ANALYTICS.STOCK_FORECAST_FINAL


This table can be consumed directly by BI dashboards.

🧱 Architecture Overview
       YFinance API
             │
             ▼
     ┌───────────────────┐
     │ Airflow ETL DAG   │
     │ daily_yfinance_   │
     │ stock_ingestion   │
     └───────────────────┘
             │
             ▼
   RAW.YFINANCE_STOCK (Snowflake)
             │
             ▼
     ┌───────────────────┐
     │ Airflow Forecast  │
     │ daily_stock_price │
     │ _forecasting DAG  │
     └───────────────────┘
             │
             ▼
    ML Model + Forecast Tables
             │
             ▼
 ANALYTICS.STOCK_FORECAST_FINAL

⚙️ Technologies Used

Apache Airflow (Docker)

Snowflake Cloud Data Platform

Snowflake Native ML Forecasting

Python 3.12

YFinance for market data ingestion

Docker Compose

Pandas

📁 Project Structure
/dags
   ├── daily_yfinance_stock_ingestion.py
   ├── daily_stock_price_forecasting.py
/docker-compose.yaml
/logs
/docs
   ├── lab_report.pdf

🔄 Airflow Pipelines
1. ETL Pipeline — daily_yfinance_stock_ingestion

Extracts latest 180-day stock data

Transforms to Snowflake-ready format

Loads into RAW.YFINANCE_STOCK

Triggers the forecasting DAG

2. Forecast Pipeline — daily_stock_price_forecasting

Creates training view

Trains Snowflake ML Forecast model

Generates 7-day predictions

Writes into final analytics table

Both DAGs use:

Airflow Variables

SnowflakeHook

BEGIN / COMMIT / ROLLBACK

TriggerDagRunOperator

🗄️ Required SQL (included in Lab Report)
✔ RAW Table
CREATE TABLE IF NOT EXISTS RAW.YFINANCE_STOCK (
  SYMBOL VARCHAR,
  DATE DATE,
  OPEN FLOAT,
  CLOSE FLOAT,
  MIN FLOAT,
  MAX FLOAT,
  VOLUME FLOAT,
  PRIMARY KEY (SYMBOL, DATE)
);

✔ Training View
CREATE OR REPLACE VIEW ANALYTICS.STOCK_VIEW AS
SELECT
  SYMBOL,
  TO_TIMESTAMP_NTZ(DATE) AS DATE_V1,
  CLOSE
FROM RAW.YFINANCE_STOCK
WHERE CLOSE IS NOT NULL;

✔ Model Creation
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.PREDICT_STOCK_PRICE(
  INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'ANALYTICS.STOCK_VIEW'),
  SERIES_COLNAME    => 'SYMBOL',
  TIMESTAMP_COLNAME => 'DATE_V1',
  TARGET_COLNAME    => 'CLOSE',
  CONFIG_OBJECT     => { 'ON_ERROR': 'SKIP' }
);

✔ Forecast Output
CREATE OR REPLACE TABLE ANALYTICS.STOCK_FORECAST_TEMP AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

✔ Final Table
CREATE OR REPLACE TABLE ANALYTICS.STOCK_FORECAST_FINAL AS
SELECT
    SYMBOL,
    DATE AS PRICE_DATE,
    CLOSE AS ACTUAL,
    NULL AS FORECAST,
    NULL AS LOWER_BOUND,
    NULL AS UPPER_BOUND
FROM RAW.YFINANCE_STOCK

UNION ALL

SELECT
    REPLACE(SERIES, '"', '') AS SYMBOL,
    TS AS PRICE_DATE,
    NULL AS ACTUAL,
    FORECAST,
    LOWER_BOUND,
    UPPER_BOUND
FROM ANALYTICS.STOCK_FORECAST_TEMP;

▶ How to Run the Project
1️⃣ Start Airflow
docker compose up --build

2️⃣ Open Airflow UI


3️⃣ Create Airflow Variables
Variable	Value
stock_ticker_list	AAPL,MSFT,GOOG
stock_table_name	RAW.YFINANCE_STOCK
train_view_name	ANALYTICS.STOCK_VIEW
forecast_table_name	ANALYTICS.STOCK_FORECAST_TEMP
forecast_function_name	ANALYTICS.PREDICT_STOCK_PRICE
final_table_name	ANALYTICS.STOCK_FORECAST_FINAL
4️⃣ Trigger ETL DAG

daily_yfinance_stock_ingestion

5️⃣ Forecast DAG runs automatically
Or trigger manually.