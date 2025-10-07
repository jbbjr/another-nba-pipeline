# Data Quality & Validation

## Overview

This document describes the data quality validation approach for the NBA ETL pipeline. The validation framework ensures data integrity, completeness, and consistency across all tables in the SQLite database.

## Validation Philosophy

**Core Principles:**
1. **Prevention over Detection** - The ETL pipeline includes defensive null handling and type validation during transformation
2. **Fail Fast** - Critical data quality issues should be caught immediately after load
3. **Actionable Results** - Validation reports include specific examples to aid debugging
4. **Layered Validation** - Checks are organized by severity (ERROR vs WARNING) and category

**When to Validate:**
- After every ETL run (automated)
- Before releasing data to downstream consumers
- When data quality issues are suspected

---

## Validation Categories

### 1. Duplicate Detection

**Objective:** Ensure no duplicate records exist in fact tables

**Checks Implemented:**

#### ✓ Play-by-Play Duplicates
```sql
SELECT game_id, action_number, COUNT(*) 
FROM fact_play_by_play
GROUP BY game_id, action_number
HAVING COUNT(*) > 1
```

**Why it matters:** Duplicate play-by-play events would inflate statistics and distort game timelines. Each action within a game should have a unique `action_number`.

**Root causes if duplicates found:**
- ETL ran multiple times without idempotent upsert logic
- Source data contains duplicates
- Primary key constraint not enforced

#### ✓ Player Game Stats Duplicates
```sql
SELECT game_id, player_id, COUNT(*) 
FROM fact_player_game_stats
GROUP BY game_id, player_id
HAVING COUNT(*) > 1
```

**Why it matters:** A player should appear exactly once per game in the stats table. Duplicates would double-count their performance.

#### ✓ Team Game Stats Duplicates
```sql
SELECT game_id, team_id, COUNT(*) 
FROM fact_team_game_stats
GROUP BY game_id, team_id
HAVING COUNT(*) > 1
```

**Why it matters:** Each team should have exactly one stat record per game.

---

### 2. Referential Integrity

**Objective:** Verify all foreign key relationships are valid

**Checks Implemented:**

#### ✓ Player Stats → Players Foreign Key
```sql
SELECT DISTINCT s.player_id
FROM fact_player_game_stats s
LEFT JOIN dim_players p ON s.player_id = p.player_id
WHERE p.player_id IS NULL
```

**Why it matters:** Every player in game stats must exist in the players dimension. Orphaned records indicate:
- Missing player data in source files
- ID mismatches between datasets
- Data synchronization issues

#### ✓ Games → Teams Foreign Key
```sql
SELECT game_id, home_team_id, away_team_id
FROM fact_games g
WHERE NOT EXISTS (SELECT 1 FROM dim_teams WHERE team_id = g.home_team_id)
   OR NOT EXISTS (SELECT 1 FROM dim_teams WHERE team_id = g.away_team_id)
```

**Why it matters:** Games must reference valid teams in both home and away positions.

#### ✓ Team Stats → Games Foreign Key
```sql
SELECT DISTINCT s.game_id
FROM fact_team_game_stats s
LEFT JOIN fact_games g ON s.game_id = g.game_id
WHERE g.game_id IS NULL
```

**Why it matters:** Team statistics should only exist for games that are in the fact_games table.

#### ✓ Games → Arenas Foreign Key
```sql
SELECT game_id, arena_id
FROM fact_games g
WHERE NOT EXISTS (SELECT 1 FROM dim_arenas WHERE arena_id = g.arena_id)
```

**Why it matters:** Every game must have a valid venue.

---

### 3. Missing or Malformed Data

**Objective:** Identify incomplete or invalid data values

**Checks Implemented:**

#### ✓ Games with Missing Scores
```sql
SELECT game_id, home_score, away_score
FROM fact_games
WHERE home_score IS NULL OR away_score IS NULL
```

**Severity:** WARNING (not ERROR)

**Why it matters:** Missing scores typically indicate future/unplayed games. This is expected for schedule data but should be monitored.

#### ✓ Negative Player Stats
```sql
SELECT game_id, player_id, points, rebounds_total, assists
FROM fact_player_game_stats
WHERE points < 0 OR rebounds_total < 0 OR assists < 0
```

**Severity:** ERROR

**Why it matters:** Negative statistics are impossible and indicate data corruption or transformation errors.

#### ✓ Players with Missing Names
```sql
SELECT player_id, first_name, last_name
FROM dim_players
WHERE first_name IS NULL OR first_name = '' 
   OR last_name IS NULL OR last_name = ''
```

**Severity:** ERROR

**Why it matters:** Every player must have a valid name for reporting and analytics.

#### ✓ Invalid Game Dates
```sql
SELECT game_id, game_datetime_est
FROM fact_games
WHERE game_datetime_est IS NULL
   OR game_datetime_est < '2000-01-01'
   OR game_datetime_est > datetime('now', '+1 year')
```

**Severity:** ERROR

**Why it matters:** Game dates must be within reasonable bounds. Dates before 2000 or more than 1 year in the future likely indicate data errors.

---

### 4. Consistency Checks

**Objective:** Ensure data is logically consistent across related tables

**Checks Implemented:**

#### ✓ Team Stats Count per Game
```sql
SELECT game_id, COUNT(*) as team_count
FROM fact_team_game_stats
GROUP BY game_id
HAVING team_count != 2
```

**Why it matters:** Every game should have exactly 2 team stat records (home and away). Deviations indicate incomplete data loads.

#### ✓ Game Score vs Team Stats
```sql
SELECT g.game_id, g.home_score, g.away_score,
       ht.points as home_team_points, at.points as away_team_points
FROM fact_games g
JOIN fact_team_game_stats ht ON g.game_id = ht.game_id AND ht.is_home_team = 1
JOIN fact_team_game_stats at ON g.game_id = at.game_id AND at.is_home_team = 0
WHERE g.home_score != ht.points OR g.away_score != at.points
```

**Why it matters:** The final scores in `fact_games` must match the points in `fact_team_game_stats`. Mismatches indicate:
- Data synchronization issues between source files
- Transformation errors
- Source data inconsistencies

#### ✓ Play-by-Play Final Score
```sql
SELECT p.game_id, 
       MAX(p.score_home) as pbp_home, 
       MAX(p.score_away) as pbp_away,
       g.home_score, g.away_score
FROM fact_play_by_play p
JOIN fact_games g ON p.game_id = g.game_id
GROUP BY p.game_id
HAVING pbp_home != g.home_score OR pbp_away != g.away_score
```

**Severity:** WARNING (not ERROR)

**Why it matters:** The final score in play-by-play should match the game's final score. Discrepancies may indicate:
- Incomplete play-by-play data
- Late game events not captured
- Overtime periods handled differently

#### ✓ Player Stats vs Roster
```sql
SELECT DISTINCT s.game_id, s.player_id, s.team_id
FROM fact_player_game_stats s
LEFT JOIN fact_player_roster r 
    ON s.player_id = r.player_id AND s.team_id = r.team_id
WHERE r.player_id IS NULL
```

**Severity:** WARNING (not ERROR)

**Why it matters:** Players appearing in games should generally be on team rosters. However, this can legitimately fail due to:
- Mid-season trades
- 10-day contracts
- Two-way players switching between teams
- Roster data snapshot vs game data timing differences

---

## Running Validation

### Command Line
```bash
# Run validation after ETL
python validate_data.py

# Exit codes:
#   0 = All checks passed
#   1 = One or more ERROR-level failures
```

### Integration with ETL Pipeline
```python
# In main.py, after load:
from validate_data import DataValidator

validator = DataValidator()
results = validator.run_all_checks()
validator.close()
```

### Sample Output
```
======================================================================
DATA QUALITY VALIDATION
======================================================================
Database: nba.db
Timestamp: 2025-01-15 14:30:22
======================================================================

[1/4] DUPLICATE DETECTION
----------------------------------------------------------------------
✓ [INFO] Play-by-Play Duplicates: No duplicate play-by-play events
✓ [INFO] Player Game Stats Duplicates: No duplicate player game stats
✓ [INFO] Team Game Stats Duplicates: No duplicate team game stats

[2/4] REFERENTIAL INTEGRITY
----------------------------------------------------------------------
✓ [INFO] Player Stats → Players FK: All player stats reference valid players
✓ [INFO] Games → Teams FK: All games reference valid teams
✓ [INFO] Team Stats → Games FK: All team stats reference valid games
✓ [INFO] Games → Arenas FK: All games reference valid arenas

[3/4] MISSING/MALFORMED DATA
----------------------------------------------------------------------
✓ [WARNING] Games with Missing Scores: Found 1316 games with missing scores (may be future games)
✓ [INFO] Negative Player Stats: No negative stats found
✓ [INFO] Players with Missing Names: All players have names
✓ [INFO] Invalid Game Dates: All game dates are valid

[4/4] CONSISTENCY CHECKS
----------------------------------------------------------------------
✓ [INFO] Team Stats Count per Game: All games have exactly 2 team stat records
✓ [INFO] Game Score vs Team Stats: Game scores match team stats
✓ [WARNING] Play-by-Play Final Score: Found 3 games where PBP scores don't match final
✓ [WARNING] Player Stats vs Roster: Found 42 player-game records not on roster (trades/signings)

======================================================================
VALIDATION SUMMARY
======================================================================
Total Checks: 15
  ✓ Passed: 15
  ✗ Failed: 0
  ⚠ Warnings: 4
  🔴 Errors: 0

✓ Data quality acceptable with minor warnings.
======================================================================
```

---

## Handling Validation Failures

### When Errors are Found

**Immediate Actions:**
1. **Do not proceed with downstream consumption** - Data is not ready
2. **Review specific examples** - Validation provides up to 5 examples per failure
3. **Check source data** - Verify parquet files have correct data
4. **Review ETL logs** - Look for transformation warnings or errors

**Common Root Causes:**

| Error Type | Likely Cause | Solution |
|------------|--------------|----------|
| Duplicates | ETL ran twice without UPSERT mode | Set `LOAD_MODE = 'UPSERT'` in config.py |
| FK Violations | Incomplete dimension loads | Ensure all source files are present |
| Negative Stats | Transformation bug | Review transform.py logic |
| Missing Names | Incomplete player data | Check players.parquet completeness |

### When Warnings are Found

**Evaluation Required:**
- Warnings indicate potential issues but may be acceptable depending on context
- Review specific cases to determine if action is needed
- Document expected warnings in operational runbooks

**Example: Missing Scores Warning**
- Expected for future scheduled games
- Action: No fix needed, but validate count matches number of unplayed games

---

## Future Enhancements

**Planned Improvements:**
1. **Statistical Validation** - Detect outliers (e.g., player scoring 200 points)
2. **Trend Analysis** - Compare current load to historical patterns
3. **Automated Alerting** - Send notifications on validation failures
4. **Data Profiling** - Track data quality metrics over time
5. **Cross-Dataset Validation** - Validate team stats = sum of player stats

---

## Conclusion

The validation framework provides comprehensive data quality assurance through:
- ✅ 15 automated checks across 4 categories
- ✅ Severity-based classification (ERROR vs WARNING)
- ✅ Actionable error messages with specific examples
- ✅ Non-zero exit codes for CI/CD integration

This ensures the NBA database maintains high data quality standards suitable for analytics and reporting.