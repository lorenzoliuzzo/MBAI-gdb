import json
import pandas as pd
from nba_api.live.nba.endpoints import PlayByPlay
from nba_api.stats.endpoints import BoxScoreTraditionalV2

from driver import get_driver
from router import get_season_path

from .period import create_periods
from .lineup import extract_starters, create_lineups


def fetch_boxscore(game_id: int):
    data = None
    try:
        boxscore = BoxScoreTraditionalV2(game_id=f"00{game_id}")
        data = boxscore.get_data_frames()[0]
    except Exception as e:
        print(f": {e}.")
    finally:
        return data
    

def fetch_pbp(game_id: int):
    pbp = PlayByPlay(game_id=f"00{game_id}").get_dict()
    df = pd.DataFrame(pbp["game"]["actions"])
    return df


def save_pbp(season_id: str, game_id: int):
    pbp_df = None
    try:
        pbp_df = fetch_pbp(game_id)
    except Exception as e: 
        print(f": {e}")

    if pbp_df is None or pbp_df.empty: 
        return 

    print(f"Successfully fetched playbyplay data for game {game_id}.")

    filename = get_season_path(season_id) / "games" / f"g{game_id}.csv"
    try:
        filename.parent.mkdir(parents=True, exist_ok=True)
        pbp_df.to_csv(filename, index=False)
    except OSError as e:
        print(f"Error creating directory {filename.parent}: {e}")
    except IOError as e:
        print(f"Error saving file {filename}: {e}")

    print(f"Successfully saved playbyplay data to {filename}.")



SET_GAME_DURATION = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, min(p.start) AS first_start, max(p.end) AS last_end
    SET 
        g.start = first_start,
        g.end = last_end,
        g.duration = duration.between(first_start, last_end)
"""


def create_game(season_id, game_id):
    driver = get_driver()
    if not driver:
        return 

    print(f"Creating a new game: {game_id}")

    filename = get_season_path(season_id) / "games" / f"g{game_id}.csv"
    game_df = None
    try: 
        game_df = pd.read_csv(filename)
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
        create_periods(session, game_id, periods_df)

        SET_GAME_DURATION_TX = lambda tx: tx.run(SET_GAME_DURATION, game_id=game_id)
        result = session.execute_write(SET_GAME_DURATION_TX)

        create_lineups(session, game_id, starters_df, subs_df)

    driver.close()