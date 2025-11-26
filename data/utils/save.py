from .fetch import fetch_pbp
import pandas as pd


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