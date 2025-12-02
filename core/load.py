from data.utils.fetch import *
from data.utils.extract import * 

from core.driver import get_driver
from core.queries import * 

from graph import get_teams


def load_teams():
    driver = get_driver()
    if not driver:
        return 

    try: 
        teams = fetch_teams()
    except Exception as e:
        print(f" : {e}")
        return

    with driver.session() as session:
        MERGE_TEAMS_TX = lambda tx: tx.run(MERGE_TEAMS, teams=teams)
        session.execute_write(MERGE_TEAMS_TX)

    driver.close()



def load_season(season_id):
    driver = get_driver()
    if not driver:
        return 

    try: 
        schedule = fetch_schedule(season_id)
    except Exception as e:
        print(f" : {e}")
        return

    with driver.session() as session:
        MERGE_SEASON_TX = lambda tx: tx.run(MERGE_SEASON, season_id=season_id, schedule=schedule)
        session.execute_write(MERGE_SEASON_TX)

    driver.close()



def load_lineups(session, game_id: int, subs: pd.DataFrame, starters: pd.DataFrame): 
    print(f"Creating `LineUp`'s for `Game` {game_id}...")
    
    ht_id, at_id = get_teams(session, game_id)

    ht_subs = subs[subs['teamId'] == ht_id]
    at_subs = subs[subs['teamId'] == at_id]

    ht_starter_ids = starters.loc[starters['TEAM_ID'] == ht_id, 'PLAYER_ID'].to_list()
    at_starter_ids = starters.loc[starters['TEAM_ID'] == at_id, 'PLAYER_ID'].to_list()

    MERGE_LINEUP_STINTS_TX = lambda tx:tx.run(MERGE_LINEUP_STINTS, game_id=game_id, 
        home_lineups=extract_lineups(ht_subs, ht_starter_ids), 
        away_lineups=extract_lineups(at_subs, at_starter_ids)
    )
    session.execute_write(MERGE_LINEUP_STINTS_TX)

    MERGE_PLAYER_STINTS_TX = lambda tx:tx.run(MERGE_PLAYER_STINTS, game_id=game_id)
    session.execute_write(MERGE_PLAYER_STINTS_TX)



def load_game(season_id, game_id):
    driver = get_driver()
    if not driver:
        return 

    print(f"Creating a new game: {game_id}")
    try: 
        boxscore_df = fetch_boxscore(game_id)
    except Exception as e: 
        print(f"Some error occured while fetching the game boxscore: {e}")
        return

    starters = extract_starters(boxscore_df)

    try: 
        pbp_df = fetch_pbp(game_id)
    except Exception as e: 
        print(f"Some error occured while reading the game actions from {filename}: {e}")
        return 

    pbp_df.sort_values(by="timeActual", ascending=True, inplace=True)

    periods = extract_periods(pbp_df)
    if len(periods) < 4: 
        return 

    subs = extract_subs(pbp_df)

    with driver.session() as session:
        print("Creating `Period` nodes...")
        MERGE_PERIODS_TX = lambda tx: tx.run(MERGE_PERIODS, game_id=game_id, periods=periods)
        session.execute_write(MERGE_PERIODS_TX)

        load_lineups(session, game_id, subs, starters)

    driver.close()



def load_lineups(session, game_id: int, subs: pd.DataFrame, starters: pd.DataFrame): 
    print(f"Creating `LineUp`'s for `Game` {game_id}...")
    
    ht_id, at_id = get_teams(session, game_id)
    ht_subs = subs[subs['teamId'] == ht_id]
    at_subs = subs[subs['teamId'] == at_id]

    ht_starter_ids = starters.loc[starters['TEAM_ID'] == ht_id, 'PLAYER_ID'].to_list()
    at_starter_ids = starters.loc[starters['TEAM_ID'] == at_id, 'PLAYER_ID'].to_list()

    ht_lineups = extract_lineups(ht_subs, ht_starter_ids)
    at_lineups = extract_lineups(at_subs, at_starter_ids)

    MERGE_LINEUPS_TX = lambda tx:tx.run(MERGE_LINEUPS, game_id=game_id, 
        home_lineups=ht_lineups, away_lineups=at_lineups
    )
    session.execute_write(MERGE_LINEUPS_TX)