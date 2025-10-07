"""Configuration for NBA ETL pipeline."""

# File paths
PARQUET_FILES = {
    'pbp': '/Users/bennett/Desktop/PBP',
    'boxscore': '/Users/bennett/Desktop/Boxscores',
    'players': '/Users/bennett/Repositories/another-nba-pipeline/2024_NBA_Players.parquet',
    'schedule': '/Users/bennett/Repositories/another-nba-pipeline/2024_NBA_Schedule.parquet'
}

DB_PATH = 'nba.db'

# ETL settings
BATCH_SIZE = 1000
DROP_EXISTING = True  # Set False for incremental loads