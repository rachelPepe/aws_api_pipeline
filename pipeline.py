"""
- loads sensitive credentials from .env
- calls public REST API to get data in JSON format
- transforms data into clean pandas DataFrame
- connects securely to an AWS RDS PostgreSQL database using psycopg2
- creates a table and uploads data into it
"""

from datetime import datetime     # used to add timestamp for when data is loaded
import requests   # for making API calls
import psycopg2   # PostgreSQL database driver (connect to AWS RDS)
from psycopg2.extras import execute_values   # Helper to bulk insert data efficiently
import pandas as pd   # for working with data easily
from dotenv import load_dotenv   # reads .env file and loads it into environment variables
import os   # allows us to work with env variables

# -------- LOAD ENVIRONMENT VARIABLES ------------------------------------

# .env file contains AWS database credentials
load_dotenv()

# retrieve them from the environment
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    raise Exception("Database credentials are missing in .env")

CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# -------- EXTRACT DATA FROM REST API --------------------------------------
def extract_crypto_data(limit=5):
    """
    calls CoinGecko API to get market data for top N coins by market cap
    :param limit: how many coins to fetch
    :return: list of dicts with coin info
    """

    # API endpoint for market data
    url = "https://api.coingecko.com/api/v3/coins/markets"

    # parameters for API request
    params = {
        "vs_currency": "usd",           # return prices in usd
        "order": "market_cap_desc",     # sort by market cap descending
        "per_page": limit,              # how many results
        "page": 1,                      # first page
        "sparkline": False              # do not include sparkline small price chart
    }

    print("Extracting crypto market data from CoinGecko...")
    response = requests.get(url, params=params)     # get request

    # If request fails (not 200 OK) raise error
    if response.status_code != 200:
        raise Exception(f"API call failed {response.status_code} = {response.text}")

    # Convert JSON response into python objects (list of dictionaries)
    data = response.json()

    # Clean and select only needed fields
    cleaned = []
    for coin in data:
        # Skip if required fields are missing
        if "id" not in coin or "current_price" not in coin or "market_cap" not in coin:
            continue

        # taking json data for each coin and putting into a dict then into list of dict's
        cleaned.append({
            "coin_id": coin.get("id"),      # unique id of coin
            "symbol": coin.get("symbol"),   # short symbol like btc
            "name": coin.get("name"),       # full name like Bitcoin
            "current_price": coin.get("current_price"), # current price in usd
            "market_cap": coin.get("market_cap")    # market cap value
        })

    print(f"Extracted {len(cleaned)} coins from API")
    # return cleaned list
    return cleaned


# --------------- TRANSFORM DATA (clean and deduplicate) ---------------------------------

def transform_crypto_data(data):
    """
    deduplicate and clean data. Adds load timestamp
    :param data: data (cleaned data)
    :return: deduped (deduplicated and clean text fields)
    """

    print("Transforming data...")

    seen = set()        # keep track of which coin_ids weve already seen
    deduped = []        # Store only unique coins

    for item in data:
        cid = item["coin_id"]   # get unique id of this coin
        if cid in seen:         # if weve seen it skip it
            continue
        seen.add(cid)

        # clean text fields
        item["symbol"] = item["symbol"].strip().lower() if item.get("symbol") else None
        item["name"] = item["name"].strip() if item.get("name") else None

        # add Timestamp so we know when data was ingested
        item["load_timestamp"] = datetime.utcnow()

        deduped.append(item)

    print(f"Deduplication complete: kept {len(deduped)} of {len(data)} records.")
    return deduped


# ------------ LOAD TO POSTGRESQL -------------------------------------

def load_to_postgres(transformed_data):
    """
    Connects to AWS RDS and inserts crypto data into table
    if table doesn't exist it will be created
    use ON CONFLICT to avoid duplicate inserts
    """

    print("Connecting to postgreSQL...")

    # open connection to DB
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS crypto_market (
        coin_id TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        current_price NUMERIC,
        market_cap BIGINT,
        load_timestamp TIMESTAMP
    );
    """)

    # build list of tuples for bulk insert
    values = []
    for item in transformed_data:
        values.append((
            item["coin_id"],
            item["symbol"],
            item["name"],
            item["current_price"],
            item["market_cap"],
            item["load_timestamp"]
        ))

    # use execute_values for efficient bulk insert
    # ON CONFLICT updates price, market_cap, and timestamp if coin_id already exists
    insert_query = """
    INSERT INTO crypto_market (coin_id, symbol, name, current_price, market_cap, load_timestamp)
    VALUES %s
    ON CONFLICT (coin_id) DO UPDATE
        SET current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            load_timestamp = EXCLUDED.load_timestamp;
    """

    execute_values(cur, insert_query, values)   # Bulk insert
    conn.commit()
    conn.close()

    print("Load complete: data inserted/updated successfully")



# ------------------- PIPELINE ORCHESTRATION ------------------------

def run_pipeline():
    """
    orchestrates whole ETL process
    """

    raw = extract_crypto_data(limit=5)
    transformed = transform_crypto_data(raw)
    load_to_postgres(transformed)
    print("Pipeline completed successfully")


# --------------------- RUN PIPELINE--------------------

if __name__ == "__main__":
    run_pipeline()







