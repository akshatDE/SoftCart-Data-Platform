from fastapi import FastAPI
from src.databases.mysqlconn import MySqlConnection
from src.datawarehouse.snowflakeconn import SnowflakeConnection
from configparser import ConfigParser
from loguru import logger
import pandas as pd 
import os
from langchain_ollama import OllamaLLM
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel




config = ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "resources", "config_file.ini")
config.read(config_path)


ollama_url = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
llm = OllamaLLM(model="sqlcoder", base_url=ollama_url)


schema = """
You are a Snowflake SQL expert. Generate a valid SQL query.

Schema (all tables in the `PUBLIC` schema of the `ANALYTICS` database):
- ANALYTICS.PUBLIC.FACT_SALES (product_id, customer_id, date_id, channel_id, promo_id, product_quantity, product_price)
- ANALYTICS.PUBLIC.DIM_PRODUCT (product_id, product_model, product_type)
- ANALYTICS.PUBLIC.DIM_CUSTOMER (customer_id, first_name, last_name, email, segment)
- ANALYTICS.PUBLIC.DIM_DATE (date_id, date, calendar_month, calendar_year)
- ANALYTICS.PUBLIC.DIM_CHANNEL (channel_id, channel_name)
- ANALYTICS.PUBLIC.DIM_PROMOTION (promo_id, promo_code, discount_percent)

RULES:
1. Always fully-qualify tables as ANALYTICS.PUBLIC.<TABLE_NAME>
2. Use simple table aliases (fs, dp, dc)
3. Return ONLY the SQL query, no markdown, no explanation
4. Start with SELECT

EXAMPLE:
Question: Top 5 products by revenue
SQL: SELECT dp.product_model, SUM(fs.product_quantity * fs.product_price) AS revenue
FROM ANALYTICS.FACT_SALES fs
JOIN ANALYTICS.DIM_PRODUCT dp ON fs.product_id = dp.product_id
GROUP BY dp.product_model
ORDER BY revenue DESC
LIMIT 5;

EXAMPLE:
Question: Revenue by customer segment
SQL: SELECT dc.segment, SUM(fs.product_quantity * fs.product_price) AS revenue
FROM ANALYTICS.FACT_SALES fs
JOIN ANALYTICS.DIM_CUSTOMER dc ON fs.customer_id = dc.customer_id
GROUP BY dc.segment;
"""

prompt = PromptTemplate.from_template("""
{schema}

Question: {question}
SQL:""")

chain = prompt | llm

app = FastAPI()

class AskRequest(BaseModel):
    question: str

class AskResponse(BaseModel):
    question: str
    sql: str
    data: list[dict]

@app.post("/ask", response_model=AskResponse)
def ask(payload: AskRequest):
    question = payload.question  # type-safe, auto-validated
    
    sql = chain.invoke({"schema": schema, "question": question}).strip()
    
    # cleanup
    import re
    sql = re.sub(r"```sql\s*|```", "", sql).strip()
    sql = sql.split(";")[0].strip() + ";"
    
    if not sql.upper().startswith("SELECT"):
        raise HTTPException(400, f"Only SELECT allowed. Generated: {sql}")
    
    engine = SnowflakeConnection.get_instance().get_engine()
    df = pd.read_sql(sql, engine)
    
    return AskResponse(
        question=question,
        sql=sql,
        data=df.to_dict(orient="records")
    )

@app.post("/ask")
def ask(payload: dict):
    sql = chain.invoke({"schema": schema, "question": payload["question"]}).strip()
    
    if not sql.upper().startswith("SELECT"):
        return {"error": "Only SELECT allowed", "sql": sql}
    
    engine = SnowflakeConnection.get_instance().get_engine()
    df = pd.read_sql(sql, engine)
    return {"sql": sql, "data": df.to_dict(orient="records")}



def run_query(query: str):
    snowflake_conn = SnowflakeConnection.get_instance()
    engine = snowflake_conn.get_engine()
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/revenue/by-category")
async def revenue_by_category():

    query = """
            SELECT dp.product_type,
                SUM(fs.product_price * fs.product_quantity) AS total_revenue
            FROM analytics.fact_sales fs
            JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
            GROUP BY dp.product_type
            ORDER BY total_revenue DESC;

            """
    return run_query(query=query)

@app.get("/trends/monthly")
async def trends_monthly():
    # Q2 - Monthly revenue by category
    query = """
        SELECT d.calendar_year, d.calendar_month, dp.product_type,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        JOIN analytics.dim_date d ON fs.date_id = d.date_id
        GROUP BY d.calendar_year, d.calendar_month, dp.product_type
        ORDER BY d.calendar_year, d.calendar_month;
    """
    return run_query(query=query)


@app.get("/customers/behavior")
async def customer_behavior():
    # Q3 - Customer purchasing behavior
    query = """
        SELECT c.customer_id, c.segment,
               COUNT(*) AS order_count,
               SUM(fs.product_price * fs.product_quantity) AS total_spend
        FROM analytics.fact_sales fs
        JOIN analytics.dim_customer c ON fs.customer_id = c.customer_id
        GROUP BY c.customer_id, c.segment;
    """
    return run_query(query=query)


@app.get("/revenue/concentration")
async def revenue_concentration():
    # Q4 - Top 15 products by revenue
    query = """
        SELECT dp.product_model,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue
        FROM analytics.fact_sales fs
        JOIN analytics.dim_product dp ON fs.product_id = dp.product_id
        GROUP BY dp.product_model
        ORDER BY total_revenue DESC
        LIMIT 15;
    """
    return run_query(query=query)

@app.get("/channels/promotions")
async def channels_promotions():
    # Q5 - Channel and promo impact
    query = """
        SELECT dc.channel_name, dp.promo_code,
               SUM(fs.product_price * fs.product_quantity) AS total_revenue,
               SUM(fs.product_quantity) AS total_quantity
        FROM analytics.fact_sales fs
        JOIN analytics.dim_channel dc ON fs.channel_id = dc.channel_id
        JOIN analytics.dim_promotion dp ON fs.promo_id = dp.promo_id
        GROUP BY dc.channel_name, dp.promo_code;
    """
    return run_query(query=query)


