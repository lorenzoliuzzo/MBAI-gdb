import json
import pandas as pd
from nba_api.live.nba.endpoints import PlayByPlay

from driver import get_driver
from router import get_season_path
import period


def fetch_pbp(game_id: int):
    pbp = PlayByPlay(game_id=f"00{game_id}")
    df = pd.DataFrame(pbp.get_dict()["game"]["actions"])
    return df


def save_pbp(season_id, game_id):
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



SET_GAME_DURATION_QUERY = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, min(p.start) AS first_start, max(p.end) AS last_end
    SET 
        g.start = first_start,
        g.end = last_end,
        g.duration = duration.between(first_start, last_end)
"""
    

def create_game(season_id, game_id):
    filename = get_season_path(season_id) / "games" / f"g{game_id}.csv"
    game_df = None
    try: 
        game_df = pd.read_csv(filename)
    except Exception as e: 
        print(f"Some error occured while reading the game actions from {filename}: {e}")
    
    periods = get_periods(game_df)

    driver = get_driver()
    if driver:
        with driver.session() as session:
            print(f"Creating `Period`s for `Game` {game_id}...")
            
            merge_periods_tx = lambda tx: tx.run(
                MERGE_PERIOD_QUERY, 
                game_id=game_id, 
                periods=periods
            )
            result = session.execute_write(merge_periods_tx)
            print(f"Successfully created periods.")

            merge_next_link_tx = lambda tx: tx.run(MERGE_NEXT_PERIOD_LINK_QUERY, game_id=game_id)
            result = session.execute_write(merge_next_link_tx)
            print(f"Successfully linked periods with `:NEXT`.")

            set_game_duration_tx = lambda tx: tx.run(SET_GAME_DURATION_QUERY, game_id=game_id)
            result = session.execute_write(set_game_duration_tx)
            print(f"Successfully added temporal info in `Game`.")

        driver.close()
        print(f"Transaction completed!")