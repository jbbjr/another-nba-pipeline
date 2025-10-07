"""Transform: Convert raw DataFrames to target schema."""

import pandas as pd
import json
from datetime import datetime


def build_dim_teams(players_df, schedule_df):
    """Build dim_teams from players and schedule data."""
    # From players
    teams_players = players_df[['teamId', 'teamName', 'teamCity', 'teamAbbreviation']].copy()
    teams_players.columns = ['team_id', 'team_name', 'team_city', 'team_tricode']
    teams_players['is_defunct'] = players_df['teamIsDefunct'].astype(bool)
    teams_players['team_slug'] = teams_players['team_name'].str.lower().str.replace(' ', '-')
    
    # From schedule
    home = schedule_df[['homeTeam.teamId', 'homeTeam.teamName', 'homeTeam.teamCity', 
                        'homeTeam.teamTricode', 'homeTeam.teamSlug']].copy()
    home.columns = ['team_id', 'team_name', 'team_city', 'team_tricode', 'team_slug']
    home['is_defunct'] = False
    
    away = schedule_df[['awayTeam.teamId', 'awayTeam.teamName', 'awayTeam.teamCity',
                        'awayTeam.teamTricode', 'awayTeam.teamSlug']].copy()
    away.columns = ['team_id', 'team_name', 'team_city', 'team_tricode', 'team_slug']
    away['is_defunct'] = False
    
    # Combine and deduplicate
    teams = pd.concat([teams_players, home, away], ignore_index=True)
    teams = teams.drop_duplicates(subset=['team_id']).sort_values('team_id')
    
    return teams[['team_id', 'team_name', 'team_city', 'team_tricode', 'team_slug', 'is_defunct']]


def build_dim_players(players_df):
    """Build dim_players from players data."""
    return players_df[[
        'playerId', 'firstName', 'lastName', 'playerSlug', 'position',
        'height', 'weight', 'birthdate', 'country', 'draftYear',
        'draftRound', 'draftNumber', 'lastAffiliation', 'lastAffiliationType'
    ]].rename(columns={
        'playerId': 'player_id',
        'firstName': 'first_name',
        'lastName': 'last_name',
        'playerSlug': 'player_slug',
        'draftYear': 'draft_year',
        'draftRound': 'draft_round',
        'draftNumber': 'draft_number',
        'lastAffiliation': 'last_affiliation',
        'lastAffiliationType': 'last_affiliation_type'
    }).drop_duplicates(subset=['player_id'])


def build_dim_arenas(schedule_df):
    """Build dim_arenas from schedule data."""
    arenas = schedule_df[['arenaName', 'arenaCity', 'arenaState']].drop_duplicates()
    arenas = arenas.reset_index(drop=True)
    arenas.index.name = 'arena_id'
    arenas = arenas.reset_index()
    
    return arenas.rename(columns={
        'arenaName': 'arena_name',
        'arenaCity': 'arena_city',
        'arenaState': 'arena_state'
    })


def build_dim_dates(schedule_df):
    """Build dim_dates from schedule data."""
    dates = pd.to_datetime(schedule_df['gameDateEst']).drop_duplicates()
    
    date_dim = pd.DataFrame({
        'full_date': dates,
        'date_id': dates.dt.strftime('%Y%m%d').astype(int),
        'year': dates.dt.year,
        'month': dates.dt.month,
        'day_of_week': dates.dt.dayofweek + 1,  # Monday=1
        'week_number': dates.dt.isocalendar().week
    })
    
    return date_dim[['date_id', 'full_date', 'year', 'month', 'day_of_week', 'week_number']]


def build_fact_player_roster(players_df):
    """Build fact_player_roster from players data."""
    roster = players_df[[
        'playerId', 'teamId', 'season', 'rosterStatus', 'fromYear', 'toYear',
        'isTwoWay', 'isTenDay', 'jerseyNum', 'seasonExperience'
    ]].copy()
    
    # Filter out rows with null season (inactive/historical players)
    roster = roster[roster['season'].notna()]
    
    return roster.rename(columns={
        'playerId': 'player_id',
        'teamId': 'team_id',
        'rosterStatus': 'roster_status',
        'fromYear': 'from_year',
        'toYear': 'to_year',
        'isTwoWay': 'is_two_way',
        'isTenDay': 'is_ten_day',
        'jerseyNum': 'jersey_num',
        'seasonExperience': 'season_experience'
    })


def build_fact_games(schedule_df, arenas_df):
    """Build fact_games from schedule data."""
    # Merge arena_id
    games = schedule_df.merge(
        arenas_df,
        left_on=['arenaName', 'arenaCity', 'arenaState'],
        right_on=['arena_name', 'arena_city', 'arena_state'],
        how='left'
    )
    
    # Handle any missing arena_ids (shouldn't happen, but defensive)
    if games['arena_id'].isna().any():
        print(f"  Warning: {games['arena_id'].isna().sum()} games with missing arena_id")
        games = games[games['arena_id'].notna()]
    
    games['game_date_id'] = pd.to_datetime(games['gameDateEst']).dt.strftime('%Y%m%d').astype(int)
    
    return games[[
        'gameId', 'gameCode', 'game_date_id', 'gameDateTimeEst', 'gameDateTimeUTC',
        'seasonType', 'gameStatus', 'gameStatusText', 'gameSequence', 'arena_id',
        'homeTeam.teamId', 'awayTeam.teamId', 'homeTeam.score', 'awayTeam.score',
        'homeTeam.wins', 'homeTeam.losses', 'homeTeam.seed',
        'awayTeam.wins', 'awayTeam.losses', 'awayTeam.seed',
        'isNeutral', 'seriesGameNumber', 'seriesText', 'seriesConference',
        'gameLabel', 'gameSubtype'
    ]].rename(columns={
        'gameId': 'game_id',
        'gameCode': 'game_code',
        'gameDateTimeEst': 'game_datetime_est',
        'gameDateTimeUTC': 'game_datetime_utc',
        'seasonType': 'season_type',
        'gameStatus': 'game_status',
        'gameStatusText': 'game_status_text',
        'gameSequence': 'game_sequence',
        'homeTeam.teamId': 'home_team_id',
        'awayTeam.teamId': 'away_team_id',
        'homeTeam.score': 'home_score',
        'awayTeam.score': 'away_score',
        'homeTeam.wins': 'home_wins',
        'homeTeam.losses': 'home_losses',
        'homeTeam.seed': 'home_seed',
        'awayTeam.wins': 'away_wins',
        'awayTeam.losses': 'away_losses',
        'awayTeam.seed': 'away_seed',
        'isNeutral': 'is_neutral',
        'seriesGameNumber': 'series_game_number',
        'seriesText': 'series_text',
        'seriesConference': 'series_conference',
        'gameLabel': 'game_label',
        'gameSubtype': 'game_subtype'
    })


def build_fact_team_game_stats(boxscore_df):
    """Build fact_team_game_stats from boxscore data."""
    home = boxscore_df[[
        'gameId', 'homeTeam_teamId',
        'homeTeam_statistics_points', 'homeTeam_statistics_fieldGoalsMade',
        'homeTeam_statistics_fieldGoalsAttempted', 'homeTeam_statistics_fieldGoalsPercentage',
        'homeTeam_statistics_threePointersMade', 'homeTeam_statistics_threePointersAttempted',
        'homeTeam_statistics_threePointersPercentage', 'homeTeam_statistics_freeThrowsMade',
        'homeTeam_statistics_freeThrowsAttempted', 'homeTeam_statistics_freeThrowsPercentage',
        'homeTeam_statistics_reboundsOffensive', 'homeTeam_statistics_reboundsDefensive',
        'homeTeam_statistics_reboundsTeam', 'homeTeam_statistics_reboundsTotal',
        'homeTeam_statistics_assists', 'homeTeam_statistics_turnovers',
        'homeTeam_statistics_steals', 'homeTeam_statistics_blocks',
        'homeTeam_statistics_foulsPersonal', 'homeTeam_statistics_pointsInThePaint',
        'homeTeam_statistics_pointsSecondChance', 'homeTeam_statistics_pointsFastBreak',
        'homeTeam_statistics_pointsFromTurnovers', 'homeTeam_statistics_foulsDrawn'
    ]].copy()
    home.columns = ['game_id', 'team_id', 'points', 'field_goals_made', 'field_goals_attempted',
                    'field_goal_pct', 'three_pointers_made', 'three_pointers_attempted',
                    'three_pointer_pct', 'free_throws_made', 'free_throws_attempted',
                    'free_throw_pct', 'rebounds_offensive', 'rebounds_defensive',
                    'rebounds_team', 'rebounds_total', 'assists', 'turnovers',
                    'steals', 'blocks', 'fouls_personal', 'points_in_paint',
                    'points_second_chance', 'points_fast_break', 'points_from_turnovers',
                    'fouls_drawn']
    home['is_home_team'] = True
    
    away = boxscore_df[[
        'gameId', 'awayTeam_teamId',
        'awayTeam_statistics_points', 'awayTeam_statistics_fieldGoalsMade',
        'awayTeam_statistics_fieldGoalsAttempted', 'awayTeam_statistics_fieldGoalsPercentage',
        'awayTeam_statistics_threePointersMade', 'awayTeam_statistics_threePointersAttempted',
        'awayTeam_statistics_threePointersPercentage', 'awayTeam_statistics_freeThrowsMade',
        'awayTeam_statistics_freeThrowsAttempted', 'awayTeam_statistics_freeThrowsPercentage',
        'awayTeam_statistics_reboundsOffensive', 'awayTeam_statistics_reboundsDefensive',
        'awayTeam_statistics_reboundsTeam', 'awayTeam_statistics_reboundsTotal',
        'awayTeam_statistics_assists', 'awayTeam_statistics_turnovers',
        'awayTeam_statistics_steals', 'awayTeam_statistics_blocks',
        'awayTeam_statistics_foulsPersonal', 'awayTeam_statistics_pointsInThePaint',
        'awayTeam_statistics_pointsSecondChance', 'awayTeam_statistics_pointsFastBreak',
        'awayTeam_statistics_pointsFromTurnovers', 'awayTeam_statistics_foulsDrawn'
    ]].copy()
    away.columns = ['game_id', 'team_id', 'points', 'field_goals_made', 'field_goals_attempted',
                    'field_goal_pct', 'three_pointers_made', 'three_pointers_attempted',
                    'three_pointer_pct', 'free_throws_made', 'free_throws_attempted',
                    'free_throw_pct', 'rebounds_offensive', 'rebounds_defensive',
                    'rebounds_team', 'rebounds_total', 'assists', 'turnovers',
                    'steals', 'blocks', 'fouls_personal', 'points_in_paint',
                    'points_second_chance', 'points_fast_break', 'points_from_turnovers',
                    'fouls_drawn']
    away['is_home_team'] = False
    
    return pd.concat([home, away], ignore_index=True)


def build_fact_player_game_stats(boxscore_df):
    """Build fact_player_game_stats from boxscore data (unpacks nested players)."""
    rows = []
    skipped_games = 0
    
    for _, game in boxscore_df.iterrows():
        game_id = game['gameId']
        game_had_players = False
        
        # Home players - handle JSON string, list, or numpy array
        home_players_data = game['homeTeam_players']
        if isinstance(home_players_data, str):
            try:
                home_players = json.loads(home_players_data)
            except (json.JSONDecodeError, TypeError):
                home_players = []
        else:
            # Handle numpy array, list, or dict
            home_players = list(home_players_data) if hasattr(home_players_data, '__iter__') else [home_players_data]
        
        for p in home_players:
            if isinstance(p, dict) and p.get('personId'):
                game_had_players = True
                # Stats are nested under 'statistics' key
                stats = p.get('statistics', {})
                rows.append({
                    'game_id': game_id,
                    'player_id': p.get('personId'),
                    'team_id': game['homeTeam_teamId'],
                    'jersey_num': p.get('jerseyNum'),
                    'position': p.get('position'),
                    'starter': p.get('starter') == '1',
                    'minutes': stats.get('minutes'),
                    'points': stats.get('points'),
                    'field_goals_made': stats.get('fieldGoalsMade'),
                    'field_goals_attempted': stats.get('fieldGoalsAttempted'),
                    'three_pointers_made': stats.get('threePointersMade'),
                    'three_pointers_attempted': stats.get('threePointersAttempted'),
                    'free_throws_made': stats.get('freeThrowsMade'),
                    'free_throws_attempted': stats.get('freeThrowsAttempted'),
                    'rebounds_offensive': stats.get('reboundsOffensive'),
                    'rebounds_defensive': stats.get('reboundsDefensive'),
                    'rebounds_total': stats.get('reboundsTotal'),
                    'assists': stats.get('assists'),
                    'turnovers': stats.get('turnovers'),
                    'steals': stats.get('steals'),
                    'blocks': stats.get('blocks'),
                    'fouls_personal': stats.get('foulsPersonal'),
                    'plus_minus': stats.get('plusMinusPoints')
                })
        
        # Away players - handle JSON string, list, or numpy array
        away_players_data = game['awayTeam_players']
        if isinstance(away_players_data, str):
            try:
                away_players = json.loads(away_players_data)
            except (json.JSONDecodeError, TypeError):
                away_players = []
        else:
            # Handle numpy array, list, or dict
            away_players = list(away_players_data) if hasattr(away_players_data, '__iter__') else [away_players_data]
        
        for p in away_players:
            if isinstance(p, dict) and p.get('personId'):
                game_had_players = True
                # Stats are nested under 'statistics' key
                stats = p.get('statistics', {})
                rows.append({
                    'game_id': game_id,
                    'player_id': p.get('personId'),
                    'team_id': game['awayTeam_teamId'],
                    'jersey_num': p.get('jerseyNum'),
                    'position': p.get('position'),
                    'starter': p.get('starter') == '1',
                    'minutes': stats.get('minutes'),
                    'points': stats.get('points'),
                    'field_goals_made': stats.get('fieldGoalsMade'),
                    'field_goals_attempted': stats.get('fieldGoalsAttempted'),
                    'three_pointers_made': stats.get('threePointersMade'),
                    'three_pointers_attempted': stats.get('threePointersAttempted'),
                    'free_throws_made': stats.get('freeThrowsMade'),
                    'free_throws_attempted': stats.get('freeThrowsAttempted'),
                    'rebounds_offensive': stats.get('reboundsOffensive'),
                    'rebounds_defensive': stats.get('reboundsDefensive'),
                    'rebounds_total': stats.get('reboundsTotal'),
                    'assists': stats.get('assists'),
                    'turnovers': stats.get('turnovers'),
                    'steals': stats.get('steals'),
                    'blocks': stats.get('blocks'),
                    'fouls_personal': stats.get('foulsPersonal'),
                    'plus_minus': stats.get('plusMinusPoints')
                })
        
        if not game_had_players:
            skipped_games += 1
    
    if skipped_games > 0:
        print(f"  Warning: {skipped_games} games had no player stats")
    
    if len(rows) == 0:
        print(f"  Warning: No player stats found - check data format")
    
    return pd.DataFrame(rows)


def build_fact_play_by_play(pbp_df):
    """Build fact_play_by_play from pbp data."""
    pbp = pbp_df[[
        'gameId', 'actionNumber', 'orderNumber', 'period', 'clock', 'timeActual',
        'teamId', 'personId', 'actionType', 'subType', 'descriptor', 'qualifiers',
        'x', 'y', 'side', 'shotDistance', 'shotResult', 'isFieldGoal',
        'scoreHome', 'scoreAway', 'possession', 'location', 'description',
        'assistPersonId', 'assistTotal', 'stealPersonId', 'turnoverTotal',
        'reboundTotal', 'foulPersonalTotal', 'foulDrawnPersonId'
    ]].copy()
    
    # Handle nulls in required fields
    pbp['location'] = pbp['location'].fillna('')
    pbp['description'] = pbp['description'].fillna('')
    
    return pbp.rename(columns={
        'gameId': 'game_id',
        'actionNumber': 'action_number',
        'orderNumber': 'order_number',
        'timeActual': 'time_actual',
        'teamId': 'team_id',
        'personId': 'player_id',
        'actionType': 'action_type',
        'subType': 'sub_type',
        'x': 'x_coord',
        'y': 'y_coord',
        'shotDistance': 'shot_distance',
        'shotResult': 'shot_result',
        'isFieldGoal': 'is_field_goal',
        'scoreHome': 'score_home',
        'scoreAway': 'score_away',
        'assistPersonId': 'assist_person_id',
        'assistTotal': 'assist_total',
        'stealPersonId': 'steal_person_id',
        'turnoverTotal': 'turnover_total',
        'reboundTotal': 'rebound_total',
        'foulPersonalTotal': 'foul_personal_total',
        'foulDrawnPersonId': 'foul_drawn_person_id'
    })


def build_fact_game_leaders(schedule_df):
    """Build fact_game_leaders from schedule data (unpacks pointsLeaders)."""
    rows = []
    
    for _, game in schedule_df.iterrows():
        game_id = game['gameId']
        points_leaders_data = game['pointsLeaders']
        
        # Handle numpy array, list, or other iterable
        if hasattr(points_leaders_data, '__iter__') and not isinstance(points_leaders_data, str):
            leaders_list = list(points_leaders_data)
            
            for leader in leaders_list:
                if isinstance(leader, dict) and leader.get('personId'):
                    rows.append({
                        'game_id': game_id,
                        'team_id': leader.get('teamId'),
                        'player_id': leader.get('personId'),
                        'stat_type': 'points',  # This dataset only has points leaders
                        'value': leader.get('points', 0)
                    })
        
        # Handle JSON string format (for backwards compatibility)
        elif isinstance(points_leaders_data, str) and points_leaders_data:
            try:
                leaders = json.loads(points_leaders_data)
                
                # Handle nested format (homeLeaders/awayLeaders)
                if isinstance(leaders, dict):
                    if 'homeLeaders' in leaders:
                        for stat_type, data in leaders['homeLeaders'].items():
                            if isinstance(data, dict) and 'personId' in data and data['personId']:
                                rows.append({
                                    'game_id': game_id,
                                    'team_id': game['homeTeam.teamId'],
                                    'player_id': data['personId'],
                                    'stat_type': stat_type,
                                    'value': data.get('value', 0)
                                })
                    
                    if 'awayLeaders' in leaders:
                        for stat_type, data in leaders['awayLeaders'].items():
                            if isinstance(data, dict) and 'personId' in data and data['personId']:
                                rows.append({
                                    'game_id': game_id,
                                    'team_id': game['awayTeam.teamId'],
                                    'player_id': data['personId'],
                                    'stat_type': stat_type,
                                    'value': data.get('value', 0)
                                })
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['game_id', 'team_id', 'player_id', 'stat_type', 'value'])


def transform_all(raw_data):
    """Execute all transformations."""
    print("Building dimensions...")
    dim_teams = build_dim_teams(raw_data['players'], raw_data['schedule'])
    dim_players = build_dim_players(raw_data['players'])
    dim_arenas = build_dim_arenas(raw_data['schedule'])
    dim_dates = build_dim_dates(raw_data['schedule'])
    
    print("Building facts...")
    
    # Track filtering for roster
    total_players = len(raw_data['players'])
    fact_player_roster = build_fact_player_roster(raw_data['players'])
    filtered_players = total_players - len(fact_player_roster)
    if filtered_players > 0:
        print(f"  Note: Filtered {filtered_players} players without season data from roster")
    
    fact_games = build_fact_games(raw_data['schedule'], dim_arenas)
    fact_team_game_stats = build_fact_team_game_stats(raw_data['boxscore'])
    fact_player_game_stats = build_fact_player_game_stats(raw_data['boxscore'])
    fact_play_by_play = build_fact_play_by_play(raw_data['pbp'])
    fact_game_leaders = build_fact_game_leaders(raw_data['schedule'])
    
    return {
        'dim_teams': dim_teams,
        'dim_players': dim_players,
        'dim_arenas': dim_arenas,
        'dim_dates': dim_dates,
        'fact_player_roster': fact_player_roster,
        'fact_games': fact_games,
        'fact_team_game_stats': fact_team_game_stats,
        'fact_player_game_stats': fact_player_game_stats,
        'fact_play_by_play': fact_play_by_play,
        'fact_game_leaders': fact_game_leaders
    }