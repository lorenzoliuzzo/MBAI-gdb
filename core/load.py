from data.utils.fetch import *
from data.utils.extract import * 
from core.queries import * 
from core.router import get_season_path
from core.driver import get_driver
from graph import get_teams


def load_periods(session, game_id, periods_df): 
    print(f"Creating `Period`s for `Game` {game_id}...")

    periods = extract_periods(periods_df)
    if len(periods) < 4: 
        return 

    MERGE_PERIOD_TX = lambda tx: tx.run(MERGE_PERIOD, 
        game_id=game_id, periods=periods
    )
    session.execute_write(MERGE_PERIOD_TX)

    MERGE_NEXT_PERIOD_LINK_TX = lambda tx: tx.run(MERGE_NEXT_PERIOD_LINK, 
        game_id=game_id
    )
    session.execute_write(MERGE_NEXT_PERIOD_LINK_TX)



def load_lineups(session, game_id: int, starters: pd.DataFrame, subs: pd.DataFrame): 
    print(f"Creating `LineUp`'s for `Game` {game_id}...")
    
    ht_id, at_id = get_teams(session, game_id)
    for team_id in [ht_id, at_id]:
        starter_ids = starters.loc[starters['TEAM_ID'] == team_id, 'PLAYER_ID'].to_list()
        team_subs = subs[subs['teamId'] == team_id]
        
        lineups = extract_lineups(starter_ids, team_subs)
        MERGE_LINEUPS_TX = lambda tx:tx.run(MERGE_LINEUPS, 
            game_id=game_id, team_id=team_id, lineups=lineups
        )
        session.execute_write(MERGE_LINEUPS_TX)

    MERGE_NEXT_LINEUP_LINK_TX = lambda tx: tx.run(MERGE_NEXT_LINEUP_LINK, game_id=game_id)
    session.execute_write(MERGE_NEXT_LINEUP_LINK_TX)



def load_game(season_id, game_id):
    driver = get_driver()
    if not driver:
        return 

    print(f"Creating a new game: {game_id}")

    # filename = get_season_path(season_id) / "games" / f"g{game_id}.csv"
    game_df = None
    try: 
        # game_df = pd.read_csv(filename)
        game_df = fetch_pbp(game_id)
    except Exception as e: 
        print(f"Some error occured while reading the game actions from {filename}: {e}")
    
    boxscore_df = None
    try: 
        boxscore_df = fetch_boxscore(game_id)
    except Exception as e: 
        print(f"Some error occured while fetching the game boxscore: {e}")

    # if not game_df or not boxscore_df: 
    #     return

    periods_mask = (game_df["actionType"] == "period")
    periods_cols = ["timeActual", "period"]
    periods_df = game_df.loc[periods_mask, periods_cols]

    subs_mask = (game_df["actionType"] == "substitution")
    subs_cols = ["timeActual", "period", "clock", "subType", "personId", "teamId"]
    subs_df = game_df.loc[subs_mask, subs_cols]

    starters_df = extract_starters(boxscore_df)
    
    with driver.session() as session:
        load_periods(session, game_id, periods_df)

        SET_GAME_DURATION_TX = lambda tx: tx.run(SET_GAME_DURATION, game_id=game_id)
        session.execute_write(SET_GAME_DURATION_TX)

        load_lineups(session, game_id, starters_df, subs_df)

    driver.close()

