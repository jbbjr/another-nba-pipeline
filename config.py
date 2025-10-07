"""Configuration for NBA ETL pipeline."""

# File paths
PARQUET_FILES = {
    'pbp': '/Users/bennett/Desktop/PBP',
    'boxscore': '/Users/bennett/Desktop/Boxscores',
    'players': '/Users/bennett/Repositories/another-nba-pipeline/2024_NBA_Players.parquet',
    'schedule': '/Users/bennett/Repositories/another-nba-pipeline/2024_NBA_Schedule.parquet'
}

DB_PATH = 'nba.db'

# ETL Mode
LOAD_MODE = 'UPSERT'  # Options: 'FULL_REFRESH', 'UPSERT'

# FULL_REFRESH: Drops and recreates all tables (fast but destructive)
# UPSERT: Merges new data with existing (idempotent, safe for re-runs)

# ETL settings
BATCH_SIZE = 30  # SQLite chunk size to avoid 999 variable limit