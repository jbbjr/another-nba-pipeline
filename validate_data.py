"""Data Quality Validation for NBA ETL Pipeline.

Validates data integrity, completeness, and consistency across all tables.
Run after ETL to ensure data quality standards are met.
"""

import sqlite3
from datetime import datetime
from config import DB_PATH


class ValidationResult:
    """Container for validation check results."""
    
    def __init__(self, check_name, category):
        self.check_name = check_name
        self.category = category
        self.passed = True
        self.message = ""
        self.details = []
        self.severity = "INFO"  # INFO, WARNING, ERROR
    
    def fail(self, message, details=None, severity="ERROR"):
        self.passed = False
        self.message = message
        self.details = details or []
        self.severity = severity
    
    def warn(self, message, details=None):
        self.passed = True
        self.message = message
        self.details = details or []
        self.severity = "WARNING"
    
    def __str__(self):
        status = "✓" if self.passed else "✗"
        return f"{status} [{self.severity}] {self.check_name}: {self.message}"


class DataValidator:
    """Runs comprehensive data quality checks on NBA database."""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.results = []
    
    def run_all_checks(self):
        """Execute all validation checks."""
        print("=" * 70)
        print("DATA QUALITY VALIDATION")
        print("=" * 70)
        print(f"Database: {self.db_path}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Run checks by category
        self.check_duplicates()
        self.check_referential_integrity()
        self.check_missing_data()
        self.check_consistency()
        
        # Print summary
        self.print_summary()
        
        return self.results
    
    def check_duplicates(self):
        """Check for duplicate records in fact tables."""
        print("\n[1/4] DUPLICATE DETECTION")
        print("-" * 70)
        
        # Check play-by-play duplicates
        result = ValidationResult("Play-by-Play Duplicates", "Duplicates")
        query = """
            SELECT game_id, action_number, COUNT(*) as cnt
            FROM fact_play_by_play
            GROUP BY game_id, action_number
            HAVING cnt > 1
        """
        duplicates = self.conn.execute(query).fetchall()
        
        if duplicates:
            result.fail(
                f"Found {len(duplicates)} duplicate play-by-play events",
                [f"Game {row[0]}, Action {row[1]}: {row[2]} copies" for row in duplicates[:5]]
            )
        else:
            result.message = "No duplicate play-by-play events"
        
        self.results.append(result)
        print(result)
        
        # Check player game stats duplicates
        result = ValidationResult("Player Game Stats Duplicates", "Duplicates")
        query = """
            SELECT game_id, player_id, COUNT(*) as cnt
            FROM fact_player_game_stats
            GROUP BY game_id, player_id
            HAVING cnt > 1
        """
        duplicates = self.conn.execute(query).fetchall()
        
        if duplicates:
            result.fail(
                f"Found {len(duplicates)} duplicate player game stats",
                [f"Game {row[0]}, Player {row[1]}: {row[2]} copies" for row in duplicates[:5]]
            )
        else:
            result.message = "No duplicate player game stats"
        
        self.results.append(result)
        print(result)
        
        # Check team game stats duplicates
        result = ValidationResult("Team Game Stats Duplicates", "Duplicates")
        query = """
            SELECT game_id, team_id, COUNT(*) as cnt
            FROM fact_team_game_stats
            GROUP BY game_id, team_id
            HAVING cnt > 1
        """
        duplicates = self.conn.execute(query).fetchall()
        
        if duplicates:
            result.fail(
                f"Found {len(duplicates)} duplicate team game stats",
                [f"Game {row[0]}, Team {row[1]}: {row[2]} copies" for row in duplicates[:5]]
            )
        else:
            result.message = "No duplicate team game stats"
        
        self.results.append(result)
        print(result)
    
    def check_referential_integrity(self):
        """Check foreign key relationships."""
        print("\n[2/4] REFERENTIAL INTEGRITY")
        print("-" * 70)
        
        # Players in game stats must exist in dim_players
        result = ValidationResult("Player Stats → Players FK", "Referential Integrity")
        query = """
            SELECT DISTINCT s.player_id
            FROM fact_player_game_stats s
            LEFT JOIN dim_players p ON s.player_id = p.player_id
            WHERE p.player_id IS NULL
        """
        orphans = self.conn.execute(query).fetchall()
        
        if orphans:
            result.fail(
                f"Found {len(orphans)} player IDs in stats without matching players",
                [f"Player ID: {row[0]}" for row in orphans[:5]]
            )
        else:
            result.message = "All player stats reference valid players"
        
        self.results.append(result)
        print(result)
        
        # Teams in games must exist in dim_teams
        result = ValidationResult("Games → Teams FK", "Referential Integrity")
        query = """
            SELECT game_id, home_team_id, away_team_id
            FROM fact_games g
            WHERE NOT EXISTS (SELECT 1 FROM dim_teams WHERE team_id = g.home_team_id)
               OR NOT EXISTS (SELECT 1 FROM dim_teams WHERE team_id = g.away_team_id)
        """
        orphans = self.conn.execute(query).fetchall()
        
        if orphans:
            result.fail(
                f"Found {len(orphans)} games with invalid team references",
                [f"Game {row[0]}: Home={row[1]}, Away={row[2]}" for row in orphans[:5]]
            )
        else:
            result.message = "All games reference valid teams"
        
        self.results.append(result)
        print(result)
        
        # Game stats must reference existing games
        result = ValidationResult("Team Stats → Games FK", "Referential Integrity")
        query = """
            SELECT DISTINCT s.game_id
            FROM fact_team_game_stats s
            LEFT JOIN fact_games g ON s.game_id = g.game_id
            WHERE g.game_id IS NULL
        """
        orphans = self.conn.execute(query).fetchall()
        
        if orphans:
            result.fail(
                f"Found {len(orphans)} team stats for non-existent games",
                [f"Game ID: {row[0]}" for row in orphans[:5]]
            )
        else:
            result.message = "All team stats reference valid games"
        
        self.results.append(result)
        print(result)
        
        # Arenas in games must exist
        result = ValidationResult("Games → Arenas FK", "Referential Integrity")
        query = """
            SELECT game_id, arena_id
            FROM fact_games g
            WHERE NOT EXISTS (SELECT 1 FROM dim_arenas WHERE arena_id = g.arena_id)
        """
        orphans = self.conn.execute(query).fetchall()
        
        if orphans:
            result.fail(
                f"Found {len(orphans)} games with invalid arena references",
                [f"Game {row[0]}: Arena={row[1]}" for row in orphans[:5]]
            )
        else:
            result.message = "All games reference valid arenas"
        
        self.results.append(result)
        print(result)
    
    def check_missing_data(self):
        """Check for missing or malformed data."""
        print("\n[3/4] MISSING/MALFORMED DATA")
        print("-" * 70)
        
        # Check for games with missing scores
        result = ValidationResult("Games with Missing Scores", "Missing Data")
        query = """
            SELECT game_id, home_score, away_score
            FROM fact_games
            WHERE home_score IS NULL OR away_score IS NULL
        """
        missing = self.conn.execute(query).fetchall()
        
        if missing:
            result.warn(
                f"Found {len(missing)} games with missing scores (may be future games)",
                [f"Game {row[0]}: Home={row[1]}, Away={row[2]}" for row in missing[:5]],
            )
        else:
            result.message = "All games have scores"
        
        self.results.append(result)
        print(result)
        
        # Check for negative stats (data quality issue)
        result = ValidationResult("Negative Player Stats", "Malformed Data")
        query = """
            SELECT game_id, player_id, points, rebounds_total, assists
            FROM fact_player_game_stats
            WHERE points < 0 OR rebounds_total < 0 OR assists < 0
        """
        invalid = self.conn.execute(query).fetchall()
        
        if invalid:
            result.fail(
                f"Found {len(invalid)} player records with negative stats",
                [f"Game {row[0]}, Player {row[1]}: pts={row[2]}, reb={row[3]}, ast={row[4]}" 
                 for row in invalid[:5]]
            )
        else:
            result.message = "No negative stats found"
        
        self.results.append(result)
        print(result)
        
        # Check for players without names
        result = ValidationResult("Players with Missing Names", "Missing Data")
        query = """
            SELECT player_id, first_name, last_name
            FROM dim_players
            WHERE first_name IS NULL OR first_name = '' 
               OR last_name IS NULL OR last_name = ''
        """
        missing = self.conn.execute(query).fetchall()
        
        if missing:
            result.fail(
                f"Found {len(missing)} players with missing names",
                [f"Player {row[0]}: '{row[1]}' '{row[2]}'" for row in missing[:5]]
            )
        else:
            result.message = "All players have names"
        
        self.results.append(result)
        print(result)
        
        # Check for invalid dates
        result = ValidationResult("Invalid Game Dates", "Malformed Data")
        query = """
            SELECT game_id, game_datetime_est
            FROM fact_games
            WHERE game_datetime_est IS NULL
               OR game_datetime_est < '2000-01-01'
               OR game_datetime_est > datetime('now', '+1 year')
        """
        invalid = self.conn.execute(query).fetchall()
        
        if invalid:
            result.fail(
                f"Found {len(invalid)} games with invalid dates",
                [f"Game {row[0]}: {row[1]}" for row in invalid[:5]]
            )
        else:
            result.message = "All game dates are valid"
        
        self.results.append(result)
        print(result)
    
    def check_consistency(self):
        """Check data consistency across tables."""
        print("\n[4/4] CONSISTENCY CHECKS")
        print("-" * 70)
        
        # Team stats should have 2 records per game (home + away)
        result = ValidationResult("Team Stats Count per Game", "Consistency")
        query = """
            SELECT game_id, COUNT(*) as team_count
            FROM fact_team_game_stats
            GROUP BY game_id
            HAVING team_count != 2
        """
        inconsistent = self.conn.execute(query).fetchall()
        
        if inconsistent:
            result.fail(
                f"Found {len(inconsistent)} games with incorrect team stat count",
                [f"Game {row[0]}: {row[1]} teams (expected 2)" for row in inconsistent[:5]]
            )
        else:
            result.message = "All games have exactly 2 team stat records"
        
        self.results.append(result)
        print(result)
        
        # Game scores should match team stats
        result = ValidationResult("Game Score vs Team Stats", "Consistency")
        query = """
            SELECT g.game_id, g.home_score, g.away_score,
                   ht.points as home_team_points, at.points as away_team_points
            FROM fact_games g
            JOIN fact_team_game_stats ht ON g.game_id = ht.game_id AND ht.is_home_team = 1
            JOIN fact_team_game_stats at ON g.game_id = at.game_id AND at.is_home_team = 0
            WHERE g.home_score != ht.points OR g.away_score != at.points
        """
        mismatches = self.conn.execute(query).fetchall()
        
        if mismatches:
            result.fail(
                f"Found {len(mismatches)} games where scores don't match team stats",
                [f"Game {row[0]}: Game({row[1]}-{row[2]}) vs Stats({row[3]}-{row[4]})" 
                 for row in mismatches[:5]]
            )
        else:
            result.message = "Game scores match team stats"
        
        self.results.append(result)
        print(result)
        
        # Play-by-play final score should match game score
        result = ValidationResult("Play-by-Play Final Score", "Consistency")
        query = """
            SELECT p.game_id, 
                   MAX(p.score_home) as pbp_home, 
                   MAX(p.score_away) as pbp_away,
                   g.home_score, g.away_score
            FROM fact_play_by_play p
            JOIN fact_games g ON p.game_id = g.game_id
            GROUP BY p.game_id, g.home_score, g.away_score
            HAVING CAST(pbp_home AS INTEGER) != g.home_score 
                OR CAST(pbp_away AS INTEGER) != g.away_score
        """
        mismatches = self.conn.execute(query).fetchall()
        
        if mismatches:
            result.warn(
                f"Found {len(mismatches)} games where PBP scores don't match final",
                [f"Game {row[0]}: PBP({row[1]}-{row[2]}) vs Final({row[3]}-{row[4]})" 
                 for row in mismatches[:5]]
            )
        else:
            result.message = "Play-by-play scores match game finals"
        
        self.results.append(result)
        print(result)
        
        # Players in game stats should be on rosters
        result = ValidationResult("Player Stats vs Roster", "Consistency")
        query = """
            SELECT DISTINCT s.game_id, s.player_id, s.team_id
            FROM fact_player_game_stats s
            LEFT JOIN fact_player_roster r 
                ON s.player_id = r.player_id 
                AND s.team_id = r.team_id
            WHERE r.player_id IS NULL
            LIMIT 100
        """
        mismatches = self.conn.execute(query).fetchall()
        
        if mismatches:
            result.warn(
                f"Found {len(mismatches)} player-game records not on roster (trades/signings)",
                [f"Game {row[0]}, Player {row[1]}, Team {row[2]}" for row in mismatches[:5]]
            )
        else:
            result.message = "All players in games are on rosters"
        
        self.results.append(result)
        print(result)
    
    def print_summary(self):
        """Print validation summary."""
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        errors = sum(1 for r in self.results if r.severity == "ERROR")
        warnings = sum(1 for r in self.results if r.severity == "WARNING")
        
        print(f"Total Checks: {total}")
        print(f"  ✓ Passed: {passed}")
        print(f"  ✗ Failed: {failed}")
        print(f"  ⚠ Warnings: {warnings}")
        print(f"  🔴 Errors: {errors}")
        
        if errors > 0:
            print("\n⚠️  CRITICAL: Data quality issues detected!")
            print("Review failed checks above and fix data issues.")
        elif warnings > 0:
            print("\n✓ Data quality acceptable with minor warnings.")
        else:
            print("\n✓ All validation checks passed!")
        
        print("=" * 70)
    
    def close(self):
        """Close database connection."""
        self.conn.close()


def main():
    """Run validation and exit with appropriate code."""
    validator = DataValidator()
    
    try:
        results = validator.run_all_checks()
        
        # Exit code: 0 if all pass, 1 if any failures
        errors = sum(1 for r in results if not r.passed and r.severity == "ERROR")
        exit_code = 1 if errors > 0 else 0
        
        exit(exit_code)
    
    finally:
        validator.close()


if __name__ == "__main__":
    main()