"""Test script to verify idempotency - safe re-runs produce identical results."""

import sqlite3
from config import DB_PATH
from main import main


def get_row_counts():
    """Get row counts from all tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    tables = [
        'dim_teams', 'dim_players', 'dim_arenas', 'dim_dates',
        'fact_player_roster', 'fact_games', 'fact_team_game_stats',
        'fact_player_game_stats', 'fact_play_by_play', 'fact_game_leaders'
    ]
    
    counts = {}
    for table in tables:
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        counts[table] = count
    
    conn.close()
    return counts


def test_idempotency():
    """Run pipeline twice and verify counts are identical."""
    print("=" * 70)
    print("IDEMPOTENCY TEST")
    print("=" * 70)
    
    print("\n[RUN 1] Running pipeline first time...")
    main()
    counts_run1 = get_row_counts()
    
    print("\n" + "=" * 70)
    print("[RUN 2] Running pipeline second time (should be idempotent)...")
    print("=" * 70)
    main()
    counts_run2 = get_row_counts()
    
    print("\n" + "=" * 70)
    print("COMPARISON")
    print("=" * 70)
    
    all_match = True
    for table in counts_run1.keys():
        run1 = counts_run1[table]
        run2 = counts_run2[table]
        match = "✓" if run1 == run2 else "✗"
        
        if run1 != run2:
            all_match = False
        
        print(f"{match} {table:30} Run1: {run1:6,} | Run2: {run2:6,}")
    
    print("\n" + "=" * 70)
    if all_match:
        print("✓ SUCCESS: Pipeline is idempotent! Re-runs produce identical results.")
    else:
        print("✗ FAILURE: Row counts differ between runs. Check upsert logic.")
    print("=" * 70)


if __name__ == "__main__":
    test_idempotency()