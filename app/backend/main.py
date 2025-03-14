import os
from typing import List, Dict

import sqlalchemy
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

load_dotenv()  # Load sensitive information from a .env file if you have one in the same directory

# ------------------------------------------------------------------------
# 1) Read environment variables: Snowflake connection info
# ------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT", "<your_account>")
SNOWFLAKE_USER      = os.environ.get("SNOWFLAKE_USER", "<your_user>")
SNOWFLAKE_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD", "<your_password>")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "DBT_DB")
SNOWFLAKE_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "DBT_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "<your_warehouse>")
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "<your_role>")

# Generate Snowflake SQLAlchemy connection string
# Note: Replace {SNOWFLAKE_ACCOUNT} with actual region and cloud platform, e.g., "xxxxxx.region.aws"
SNOWFLAKE_CONN_STR = (
    f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
    f"{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
)

# ------------------------------------------------------------------------
# 2) Create SQLAlchemy Engine
# ------------------------------------------------------------------------
engine = create_engine(SNOWFLAKE_CONN_STR)

# ------------------------------------------------------------------------
# 3) Create FastAPI instance
# ------------------------------------------------------------------------
app = FastAPI(title="Snowflake + FastAPI: Raw vs JSON vs RDBMS Demo")

# ------------------------------------------------------------------------
# 4) Health check or root route
# ------------------------------------------------------------------------
@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI + Snowflake (SQLAlchemy) Demo!"}

# ------------------------------------------------------------------------
# 5) Demo: Query Raw Staging Table
#    Assume table name: raw_staging (you need to create and load data into this table in Snowflake first)
# ------------------------------------------------------------------------
@app.get("/raw_storage", response_model=List[Dict])
def get_raw_storage(limit: int = 10):
    """Demo querying data from a 'raw_staging' table."""
    try:
        with engine.connect() as conn:
            sql_text = text(f"SELECT * FROM DBT_DB.RAW_SCHEMA.raw_data LIMIT {limit}")
            result = conn.execute(sql_text)
            rows = result.mappings().all()  # Returns a dictionary mapping of each row
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------------
# 6) Demo: Query JSON Table
#    Assume table name: json_sec_data (you need to create a table with JSON or VARIANT columns in Snowflake)
# ------------------------------------------------------------------------
@app.get("/json_data", response_model=List[Dict])
def get_json_data(limit: int = 10):
    """Demo querying data from 'json_sec_data' storing JSON."""
    try:
        with engine.connect() as conn:
            sql_text = text(f"SELECT * FROM DBT_DB.JSON_SCHEMA.json_sec_data LIMIT {limit}")
            result = conn.execute(sql_text)
            rows = result.mappings().all()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------------
# 7) Demo: Query Denormalized Fact Table
#    Assume fact tables: balance_sheet, income_statement, cash_flow
# ------------------------------------------------------------------------
@app.get("/denormalized/balance_sheet", response_model=List[Dict])
def get_balance_sheet(limit: int = 10):
    """Query from 'balance_sheet', a typical RDBMS (Denormalized Fact) table."""
    try:
        with engine.connect() as conn:
            sql_text = text(f"SELECT * FROM balance_sheet LIMIT {limit}")
            result = conn.execute(sql_text)
            rows = result.mappings().all()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/denormalized/income_statement", response_model=List[Dict])
def get_income_statement(limit: int = 10):
    """Query from 'income_statement' table."""
    try:
        with engine.connect() as conn:
            sql_text = text(f"SELECT * FROM income_statement LIMIT {limit}")
            result = conn.execute(sql_text)
            rows = result.mappings().all()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/denormalized/cash_flow", response_model=List[Dict])
def get_cash_flow(limit: int = 10):
    """Query from 'cash_flow' table."""
    try:
        with engine.connect() as conn:
            sql_text = text(f"SELECT * FROM cash_flow LIMIT {limit}")
            result = conn.execute(sql_text)
            rows = result.mappings().all()
            return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------------
# 8) Additional Interface: Simple Comparison of Raw vs JSON vs RDBMS
# ------------------------------------------------------------------------
@app.get("/comparison")
def compare_storage():
    """
    Returns a JSON segment describing the pros and cons of three storage methods
    This is a demonstration, you could also retrieve comparison conclusions from the database, or send them for frontend rendering
    """
    comparison_info = {
        "raw_storage": {
            "advantages": [
                "Data retained in original form",
                "Traceable before ETL"
            ],
            "disadvantages": [
                "Low query efficiency",
                "Requires extensive JOINs or later processing"
            ]
        },
        "json_storage": {
            "advantages": [
                "Flexible structure",
                "Convenient for storing semi-structured data"
            ],
            "disadvantages": [
                "Requires Snowflake VARIANT column",
                "Requires handling JSON paths during query"
            ]
        },
        "denormalized_rdbms": {
            "advantages": [
                "High query efficiency",
                "Clear and easy-to-understand data model"
            ],
            "disadvantages": [
                "Tedious updates",
                "Can lead to data redundancy"
            ]
        }
    }
    return {
        "message": "Simple comparison of Raw vs JSON vs Denormalized RDBMS",
        "details": comparison_info
    }
