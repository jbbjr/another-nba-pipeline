"""Load: Create SQLite schema and insert data."""

import sqlite3
from config import DB_PATH, LOAD_MODE, BATCH_SIZE


DDL = {
    'dim_teams': """
        CREATE TABLE IF NOT EXISTS dim_teams (
            team_id INTEGER PRIMARY KEY,
            team_name TEXT NOT NULL,
            team_city TEXT NOT NULL,
            team_tricode TEXT NOT NULL,
            team_slug TEXT NOT NULL,
            is_defunct BOOLEAN NOT NULL
        )
    """,
    
    'dim_players': """
        CREATE TABLE IF NOT EXISTS dim_players (
            player_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            player_slug TEXT NOT NULL,
            position TEXT,
            height TEXT,
            weight TEXT,
            birthdate DATE,
            country TEXT,
            draft_year INTEGER,
            draft_round INTEGER,
            draft_number INTEGER,
            last_affiliation TEXT,
            last_affiliation_type TEXT
        )
    """,
    
    'dim_arenas': """
        CREATE TABLE IF NOT EXISTS dim_arenas (
            arena_id INTEGER PRIMARY KEY,
            arena_name TEXT NOT NULL,
            arena_city TEXT NOT NULL,
            arena_state TEXT
        )
    """,
    
    'dim_dates': """
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_id INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            week_number INTEGER NOT NULL
        )
    """,
    
    'fact_player_roster': """
        CREATE TABLE IF NOT EXISTS fact_player_roster (
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            season TEXT NOT NULL,
            roster_status INTEGER NOT NULL,
            from_year INTEGER NOT NULL,
            to_year INTEGER NOT NULL,
            is_two_way BOOLEAN,
            is_ten_day BOOLEAN,
            jersey_num TEXT,
            season_experience INTEGER,
            PRIMARY KEY (player_id, team_id, season),
            FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
        )
    """,
    
    'fact_games': """
        CREATE TABLE IF NOT EXISTS fact_games (
            game_id TEXT PRIMARY KEY,
            game_code TEXT NOT NULL,
            game_date_id INTEGER NOT NULL,
            game_datetime_est TIMESTAMP NOT NULL,
            game_datetime_utc TIMESTAMP NOT NULL,
            season_type TEXT NOT NULL,
            game_status INTEGER NOT NULL,
            game_status_text TEXT NOT NULL,
            game_sequence INTEGER NOT NULL,
            arena_id INTEGER NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_score INTEGER NOT NULL,
            away_score INTEGER NOT NULL,
            home_wins INTEGER NOT NULL,
            home_losses INTEGER NOT NULL,
            home_seed INTEGER,
            away_wins INTEGER NOT NULL,
            away_losses INTEGER NOT NULL,
            away_seed INTEGER,
            is_neutral BOOLEAN NOT NULL,
            series_game_number TEXT,
            series_text TEXT,
            series_conference TEXT,
            game_label TEXT,
            game_subtype TEXT,
            FOREIGN KEY (game_date_id) REFERENCES dim_dates(date_id),
            FOREIGN KEY (arena_id) REFERENCES dim_arenas(arena_id),
            FOREIGN KEY (home_team_id) REFERENCES dim_teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES dim_teams(team_id)
        )
    """,
    
    'fact_team_game_stats': """
        CREATE TABLE IF NOT EXISTS fact_team_game_stats (
            game_id TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            is_home_team BOOLEAN NOT NULL,
            points INTEGER NOT NULL,
            field_goals_made INTEGER NOT NULL,
            field_goals_attempted INTEGER NOT NULL,
            field_goal_pct REAL,
            three_pointers_made INTEGER NOT NULL,
            three_pointers_attempted INTEGER NOT NULL,
            three_pointer_pct REAL,
            free_throws_made INTEGER NOT NULL,
            free_throws_attempted INTEGER NOT NULL,
            free_throw_pct REAL,
            rebounds_offensive INTEGER NOT NULL,
            rebounds_defensive INTEGER NOT NULL,
            rebounds_team INTEGER NOT NULL,
            rebounds_total INTEGER NOT NULL,
            assists INTEGER NOT NULL,
            turnovers INTEGER NOT NULL,
            steals INTEGER NOT NULL,
            blocks INTEGER NOT NULL,
            fouls_personal INTEGER NOT NULL,
            points_in_paint INTEGER NOT NULL,
            points_second_chance INTEGER NOT NULL,
            points_fast_break INTEGER NOT NULL,
            points_from_turnovers INTEGER NOT NULL,
            fouls_drawn INTEGER NOT NULL,
            PRIMARY KEY (game_id, team_id),
            FOREIGN KEY (game_id) REFERENCES fact_games(game_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
        )
    """,
    
    'fact_player_game_stats': """
        CREATE TABLE IF NOT EXISTS fact_player_game_stats (
            game_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            jersey_num TEXT,
            position TEXT,
            starter BOOLEAN NOT NULL,
            minutes INTEGER,
            points INTEGER,
            field_goals_made INTEGER,
            field_goals_attempted INTEGER,
            three_pointers_made INTEGER,
            three_pointers_attempted INTEGER,
            free_throws_made INTEGER,
            free_throws_attempted INTEGER,
            rebounds_offensive INTEGER,
            rebounds_defensive INTEGER,
            rebounds_total INTEGER,
            assists INTEGER,
            turnovers INTEGER,
            steals INTEGER,
            blocks INTEGER,
            fouls_personal INTEGER,
            plus_minus INTEGER,
            PRIMARY KEY (game_id, player_id),
            FOREIGN KEY (game_id) REFERENCES fact_games(game_id),
            FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams(team_id)
        )
    """,
    
    'fact_play_by_play': """
        CREATE TABLE IF NOT EXISTS fact_play_by_play (
            game_id TEXT NOT NULL,
            action_number INTEGER NOT NULL,
            order_number INTEGER NOT NULL,
            period INTEGER NOT NULL,
            clock TEXT NOT NULL,
            time_actual TIMESTAMP NOT NULL,
            team_id INTEGER,
            player_id INTEGER,
            action_type TEXT NOT NULL,
            sub_type TEXT,
            descriptor TEXT,
            qualifiers TEXT,
            x_coord REAL,
            y_coord REAL,
            side TEXT,
            shot_distance REAL,
            shot_result TEXT,
            is_field_goal BOOLEAN NOT NULL,
            score_home INTEGER NOT NULL,
            score_away INTEGER NOT NULL,
            possession INTEGER NOT NULL,
            location TEXT NOT NULL,
            description TEXT NOT NULL,
            assist_person_id INTEGER,
            assist_total REAL,
            steal_person_id INTEGER,
            turnover_total REAL,
            rebound_total REAL,
            foul_personal_total REAL,
            foul_drawn_person_id INTEGER,
            PRIMARY KEY (game_id, action_number),
            FOREIGN KEY (game_id) REFERENCES fact_games(game_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams(team_id),
            FOREIGN KEY (player_id) REFERENCES dim_players(player_id)
        )
    """,
    
    'fact_game_leaders': """
        CREATE TABLE IF NOT EXISTS fact_game_leaders (
            game_id TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            stat_type TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (game_id, team_id, player_id, stat_type),
            FOREIGN KEY (game_id) REFERENCES fact_games(game_id),
            FOREIGN KEY (team_id) REFERENCES dim_teams(team_id),
            FOREIGN KEY (player_id) REFERENCES dim_players(player_id)
        )
    """
}


INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_teams_tricode ON dim_teams(team_tricode)",
    "CREATE INDEX IF NOT EXISTS idx_players_name ON dim_players(last_name, first_name)",
    "CREATE INDEX IF NOT EXISTS idx_games_date ON fact_games(game_date_id)",
    "CREATE INDEX IF NOT EXISTS idx_games_home_team ON fact_games(home_team_id, game_date_id)",
    "CREATE INDEX IF NOT EXISTS idx_games_away_team ON fact_games(away_team_id, game_date_id)",
    "CREATE INDEX IF NOT EXISTS idx_roster_team_season ON fact_player_roster(team_id, season)",
    "CREATE INDEX IF NOT EXISTS idx_player_stats_player ON fact_player_game_stats(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_pbp_period ON fact_play_by_play(game_id, period, order_number)",
    "CREATE INDEX IF NOT EXISTS idx_pbp_player ON fact_play_by_play(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_leaders_player_stat ON fact_game_leaders(player_id, stat_type)"
]


def create_schema(conn):
    """Create all tables and indexes if they don't exist."""
    cursor = conn.cursor()
    
    # Create tables
    for table, ddl in DDL.items():
        cursor.execute(ddl)
    
    # Create indexes
    for index_sql in INDEXES:
        cursor.execute(index_sql)
    
    conn.commit()


def upsert_dimensions(conn, tables):
    """Upsert dimension tables: DELETE existing keys, then INSERT."""
    cursor = conn.cursor()
    
    # Simple dimensions with single-column PKs
    simple_dims = {
        'dim_teams': 'team_id',
        'dim_players': 'player_id',
        'dim_arenas': 'arena_id',
        'dim_dates': 'date_id'
    }
    
    for table_name, pk_col in simple_dims.items():
        if table_name not in tables:
            continue
        
        df = tables[table_name]
        pk_values = df[pk_col].unique().tolist()
        
        # Delete existing records
        placeholders = ','.join('?' * len(pk_values))
        delete_sql = f"DELETE FROM {table_name} WHERE {pk_col} IN ({placeholders})"
        cursor.execute(delete_sql, pk_values)
        deleted = cursor.rowcount
        
        # Insert new records
        print(f"Upserting {table_name}: {len(df)} rows ({deleted} updated, {len(df)-deleted} new)")
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=BATCH_SIZE)
    
    # Complex dimension with composite PK
    if 'fact_player_roster' in tables:
        df = tables['fact_player_roster']
        
        # Get unique combinations
        roster_keys = df[['player_id', 'team_id', 'season']].drop_duplicates()
        
        # Delete existing combinations
        for _, row in roster_keys.iterrows():
            delete_sql = """
                DELETE FROM fact_player_roster 
                WHERE player_id = ? AND team_id = ? AND season = ?
            """
            cursor.execute(delete_sql, (row['player_id'], row['team_id'], row['season']))
        
        deleted = cursor.rowcount
        print(f"Upserting fact_player_roster: {len(df)} rows ({deleted} updated)")
        df.to_sql('fact_player_roster', conn, if_exists='append', index=False, chunksize=BATCH_SIZE)
    
    conn.commit()


def upsert_facts(conn, tables):
    """Upsert fact tables: DELETE existing records for game_ids, then INSERT."""
    # Get list of game_ids being loaded
    game_ids = []
    if 'fact_games' in tables:
        game_ids = tables['fact_games']['game_id'].unique().tolist()
    
    if not game_ids:
        print("  No games to load")
        return
    
    print(f"  Upserting data for {len(game_ids)} games")
    
    # Define fact tables and their game_id column
    fact_tables = [
        'fact_games',
        'fact_team_game_stats', 
        'fact_player_game_stats',
        'fact_play_by_play',
        'fact_game_leaders'
    ]
    
    cursor = conn.cursor()
    
    # Delete existing records for these game_ids
    placeholders = ','.join('?' * len(game_ids))
    for table_name in fact_tables:
        if table_name not in tables:
            continue
        
        delete_sql = f"DELETE FROM {table_name} WHERE game_id IN ({placeholders})"
        cursor.execute(delete_sql, game_ids)
        deleted_count = cursor.rowcount
        
        if deleted_count > 0:
            print(f"  Deleted {deleted_count} existing rows from {table_name}")
    
    conn.commit()
    
    # Insert new records
    for table_name in fact_tables:
        if table_name not in tables:
            continue
            
        df = tables[table_name]
        print(f"  Inserting {len(df)} rows into {table_name}")
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=BATCH_SIZE)
    
    conn.commit()


def full_refresh(conn, tables):
    """Drop all tables and reload everything (destructive but fast)."""
    cursor = conn.cursor()
    
    print("  Dropping all tables...")
    # Drop in reverse dependency order
    table_names = list(DDL.keys())
    table_names.reverse()
    for table in table_names:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    conn.commit()
    
    # Recreate schema
    print("  Creating schema...")
    create_schema(conn)
    
    # Load all data
    load_order = [
        'dim_teams', 'dim_players', 'dim_arenas', 'dim_dates',
        'fact_player_roster', 'fact_games', 'fact_team_game_stats',
        'fact_player_game_stats', 'fact_play_by_play', 'fact_game_leaders'
    ]
    
    for table_name in load_order:
        if table_name not in tables:
            continue
        df = tables[table_name]
        print(f"  Loading {table_name}: {len(df)} rows")
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=BATCH_SIZE)
    
    conn.commit()


def load_all(tables):
    """Create schema and load all data based on LOAD_MODE."""
    conn = sqlite3.connect(DB_PATH)
    
    try:
        if LOAD_MODE == 'FULL_REFRESH':
            print(f"Mode: FULL_REFRESH (destructive)")
            full_refresh(conn, tables)
        
        elif LOAD_MODE == 'UPSERT':
            print(f"Mode: UPSERT (idempotent)")
            
            # Ensure schema exists
            create_schema(conn)
            
            # Upsert dimensions (replace existing)
            print("\nUpserting dimensions...")
            upsert_dimensions(conn, tables)
            
            # Upsert facts (delete + insert for game_ids)
            print("\nUpserting facts...")
            upsert_facts(conn, tables)
        
        else:
            raise ValueError(f"Unknown LOAD_MODE: {LOAD_MODE}")
        
        print("\nETL complete!")
        
        # Quick validation
        cursor = conn.cursor()
        print("\nFinal row counts:")
        for table in DDL.keys():
            count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} rows")
        
    finally:
        conn.close()