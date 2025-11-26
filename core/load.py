from data.utils.fetch import *
from data.utils.extract import * 

from core.queries import * 
from core.driver import get_driver

from graph import get_teams


def load_periods(session, game_id, periods): 
    print(f"Creating `Period`s for `Game` {game_id}...")

    if len(periods) < 4: 
        return 

    MERGE_PERIOD_TX = lambda tx: tx.run(MERGE_PERIOD, game_id=game_id, periods=periods)
    session.execute_write(MERGE_PERIOD_TX)

    MERGE_NEXT_PERIOD_LINK_TX = lambda tx: tx.run(MERGE_NEXT_PERIOD_LINK, game_id=game_id)
    session.execute_write(MERGE_NEXT_PERIOD_LINK_TX)

    SET_GAME_DURATION_TX = lambda tx: tx.run(SET_GAME_DURATION, game_id=game_id)
    session.execute_write(SET_GAME_DURATION_TX)


def load_lineups(session, game_id: int, subs: pd.DataFrame, starters: pd.DataFrame): 
    print(f"Creating `LineUp`'s for `Game` {game_id}...")
    
    ht_id, at_id = get_teams(session, game_id)
    for team_id in [ht_id, at_id]:
        starter_ids = starters.loc[starters['TEAM_ID'] == team_id, 'PLAYER_ID'].to_list()
        team_subs = subs[subs['teamId'] == team_id]
        
        lineups = extract_lineups(starter_ids, team_subs)
        MERGE_LINEUPS_TX = lambda tx:tx.run(MERGE_LINEUPS, game_id=game_id, 
            team_id=team_id, lineups=lineups
        )
        session.execute_write(MERGE_LINEUPS_TX)

    MERGE_NEXT_LINEUP_LINK_TX = lambda tx: tx.run(MERGE_NEXT_LINEUP_LINK, game_id=game_id)
    session.execute_write(MERGE_NEXT_LINEUP_LINK_TX)


def load_game(season_id, game_id):
    driver = get_driver()
    if not driver:
        return 

    print(f"Creating a new game: {game_id}")

    pbp_df = None
    try: 
        pbp_df = fetch_pbp(game_id)
    except Exception as e: 
        print(f"Some error occured while reading the game actions from {filename}: {e}")
        return 

    boxscore_df = None
    try: 
        boxscore_df = fetch_boxscore(game_id)
    except Exception as e: 
        print(f"Some error occured while fetching the game boxscore: {e}")
        return
    
    periods = extract_periods(pbp_df)
    subs = extract_subs(pbp_df)
    starters = extract_starters(boxscore_df)

    with driver.session() as session:
        load_periods(session, game_id, periods)
        load_lineups(session, game_id, subs, starters)

    driver.close()