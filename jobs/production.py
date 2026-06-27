"""
chess_pipeline.py

Ingests Chess.com leaderboard and player stats into PostgreSQL
bronze schema using PySpark and JDBC.

Source:  https://api.chess.com/pub/leaderboards
Target:  PostgreSQL → bronze schema
Author:  Kudzaishe Manyanya
Created: 2026-06-19
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
import pandas as pd
import logging

# ── LOGGING ───────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────
# centralized config — update here only if values change
CONFIG = {
    "jdbc_url":       "jdbc:postgresql://postgres:5432/chess_analytics",
    "db_user":        "airflow",
    "db_password":    "airflow",
    "db_driver":      "org.postgresql.Driver",
    "schema":         "bronze",
    "headers": {
        # Chess.com requires a descriptive User-Agent or returns 403
        "User-Agent": "Manyanya (contact: kudzaishemanyanya@gmail.com)"
    },
    "leaderboard_url": "https://api.chess.com/pub/leaderboards",
    "player_url":      "https://api.chess.com/pub/player/manyanya/stats"
}

# columns returned by API but not needed for analysis
DROP_COLUMNS = ["trend_score", "trend_rank"]

# ── SPARK SESSION ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Chess Analytics") \
    .config("spark.jars", "/opt/spark/external-jars/postgresql.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── DATABASE CONNECTION ───────────────────────────────────
connection_properties = {
    "user":     CONFIG["db_user"],
    "password": CONFIG["db_password"],
    "driver":   CONFIG["db_driver"]
}

def get_data(url: str) -> dict | None:
    """
    Fetches JSON data from Chess.com API.

    Args:
        url (str): API endpoint URL.

    Returns:
        dict: Parsed JSON if successful, None if request fails.
    """
    try:
        response = requests.get(url, headers=CONFIG["headers"], timeout=10)
        response.raise_for_status()
        log.info(f"Fetched data from {url}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP error: {e}")
    except requests.exceptions.ConnectionError as e:
        log.error(f"Connection error: {e}")
    except requests.exceptions.Timeout:
        # API occasionally slow — log and return None rather than crash
        log.error(f"Timeout fetching {url}")
    return None

def write_to_postgres(df, table_name: str, write_mode: str = "overwrite") -> None:
    """
    Writes a Spark DataFrame to PostgreSQL bronze schema.

    Args:
        df:          Spark DataFrame to write.
        table_name:  Target table name (without schema prefix).
        write_mode:  'append' or 'overwrite'. Defaults to 'overwrite'.
    """
    full_table = f"{CONFIG['schema']}.{table_name}"
    try:
        df.write.jdbc(
            url=CONFIG["jdbc_url"],
            table=full_table,
            mode=write_mode,
            properties=connection_properties
        )
        log.info(f"Wrote {df.count()} rows → {full_table}")
    except Exception as e:
        log.error(f"Failed to write {full_table}: {e}")
        raise  # re-raise so Airflow marks task as failed

def process_leaderboards(data: dict) -> dict:
    """
    Converts raw leaderboard API response into Spark DataFrames.

    Args:
        data (dict): Raw JSON from Chess.com leaderboards endpoint.

    Returns:
        dict: Keys are category names, values are Spark DataFrames.
    """
    spark_dfs = {}

    for category, players in data.items():
        try:
            pdf = pd.DataFrame(players)

            # remove columns not needed for analysis
            pdf = pdf.drop(columns=[c for c in DROP_COLUMNS if c in pdf.columns])

            spark_dfs[category] = spark.createDataFrame(pdf).orderBy("rank")
            log.info(f"Processed {category} — {len(players)} players")
        except Exception as e:
            log.error(f"Failed to process {category}: {e}")

    return spark_dfs

def process_player_stats(kue: dict):
    """
    Extracts and flattens player stats from nested API response.

    Args:
        kue (dict): Raw JSON from Chess.com player stats endpoint.

    Returns:
        tuple: (kue_daily, kue_rapid, kue_blitz) Spark DataFrames.
    """
    # json_normalize flattens nested dict to dot-notation columns
    # e.g. chess_daily.last.rating becomes a single column
    kue_data = pd.json_normalize(kue)
    sp_kue   = spark.createDataFrame(kue_data)

    # backticks required — column names contain dots
    kue_daily = sp_kue.select(
        F.col("`chess_daily.last.rating`").alias("daily_last_rating"),
        F.col("`chess_daily.last.date`").alias("daily_last_date"),
        F.col("`chess_daily.best.rating`").alias("daily_best_rating"),
        F.col("`chess_daily.record.win`").alias("daily_record_win"),
        F.col("`chess_daily.record.loss`").alias("daily_record_loss"),
        F.col("`chess_daily.record.draw`").alias("daily_record_draw")
    )

    kue_rapid = sp_kue.select(
        F.col("`chess_rapid.last.rating`").alias("rapid_last_rating"),
        F.col("`chess_rapid.last.date`").alias("rapid_last_date"),
        F.col("`chess_rapid.best.rating`").alias("rapid_best_rating"),
        F.col("`chess_rapid.record.win`").alias("rapid_record_win"),
        F.col("`chess_rapid.record.loss`").alias("rapid_record_loss"),
        F.col("`chess_rapid.record.draw`").alias("rapid_record_draw")
    )

    kue_blitz = sp_kue.select(
        F.col("`chess_blitz.last.rating`").alias("blitz_last_rating"),
        F.col("`chess_blitz.last.date`").alias("blitz_last_date"),
        F.col("`chess_blitz.best.rating`").alias("blitz_best_rating"),
        F.col("`chess_blitz.record.win`").alias("blitz_record_win"),
        F.col("`chess_blitz.record.loss`").alias("blitz_record_loss"),
        F.col("`chess_blitz.record.draw`").alias("blitz_record_draw")
    )

    return kue_daily, kue_rapid, kue_blitz

def main():
    """
    Pipeline entry point. Orchestrates fetch, transform and load.
    """
    # ── EXTRACT ───────────────────────────────────────────
    data = get_data(CONFIG["leaderboard_url"])
    kue  = get_data(CONFIG["player_url"])

    if not data or not kue:
        log.error("API fetch failed. Aborting pipeline.")
        return

    # ── TRANSFORM ─────────────────────────────────────────
    spark_dfs = process_leaderboards(data)
    kue_daily, kue_rapid, kue_blitz = process_player_stats(kue)

    # ── LOAD ──────────────────────────────────────────────
    # player personal stats
    player_tables = {
        "kue_daily": kue_daily,
        "kue_rapid": kue_rapid,
        "kue_blitz": kue_blitz
    }
    for table_name, df in player_tables.items():
        write_to_postgres(df, table_name, write_mode="append")

    # global leaderboards — use .get() to avoid KeyError if category missing
    leaderboard_tables = {
        "daily_leaderboards":      spark_dfs.get("daily"),
        "live_rapid_leaderboards": spark_dfs.get("live_rapid"),
        "live_blitz_leaderboards": spark_dfs.get("live_blitz")
    }
    for table_name, df in leaderboard_tables.items():
        if df is not None:
            write_to_postgres(df, table_name, write_mode="append")

    spark.stop()
    log.info("Pipeline complete")

# ── ENTRY POINT ───────────────────────────────────────────
# prevents main() from running if file is imported by another module
if __name__ == "__main__":
    main()