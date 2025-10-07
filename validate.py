"""Validation queries for NBA database."""

import sqlite3
from config import DB_PATH


def run_sample_queries():
    """Run sample validation queries."""
    conn = sqlite3.connect(DB_PATH)
    
    queries = {
        "Top 5 scorers in a game": """
            SELECT 
                p.first_name || ' ' || p.last_name as player,
                t.team_tricode,
                s.points,
                s.rebounds_total,
                s.assists
            FROM fact_player_game_stats s
            JOIN dim_players p ON s.player_id = p.player_id
            JOIN dim_teams t ON s.team_id = t.team_id
            ORDER BY s.points DESC
            LIMIT 5
        """,
        
        "Team with most points in a game": """
            SELECT 
                t.team_name,
                ts.points,
                g.game_datetime_est
            FROM fact_team_game_stats ts
            JOIN dim_teams t ON ts.team_id = t.team_id
            JOIN fact_games g ON ts.game_id = g.game_id
            ORDER BY ts.points DESC
            LIMIT 1
        """,
        
        "Play-by-play sample": """
            SELECT 
                period,
                clock,
                description
            FROM fact_play_by_play
            WHERE game_id = (SELECT game_id FROM fact_games LIMIT 1)
            ORDER BY order_number
            LIMIT 5
        """,
        
        "Games by arena": """
            SELECT 
                a.arena_name,
                a.arena_city,
                COUNT(*) as games_hosted
            FROM fact_games g
            JOIN dim_arenas a ON g.arena_id = a.arena_id
            GROUP BY a.arena_id
            ORDER BY games_hosted DESC
            LIMIT 5
        """
    }
    
    for name, query in queries.items():
        print(f"\n{'='*60}")
        print(f"Query: {name}")
        print('='*60)
        
        cursor = conn.execute(query)
        results = cursor.fetchall()
        
        for row in results:
            print(row)
    
    conn.close()


if __name__ == "__main__":
    run_sample_queries()