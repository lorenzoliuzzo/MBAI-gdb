from tqdm import tqdm
from time import sleep
import pandas as pd
from nba_api.stats.endpoints import CommonAllPlayers, CommonPlayerInfo
from typing import Tuple, List

from driver import get_driver
from router import get_data_path


def fetch_player_ids(season_id) -> List[str]:
    players = CommonAllPlayers(season=season_id, is_only_current_season=1)
    players_df = players.get_data_frames()[0]
    player_ids = players_df["PERSON_ID"].astype("string").to_list()
    return player_ids


def fetch_player_info(player_id): 
    info_df = CommonPlayerInfo(player_id=player_id).get_data_frames()[0]
    cols2keep = [
        "FIRST_NAME", "LAST_NAME", "BIRTHDATE",
        "HEIGHT", "WEIGHT", "POSITION",
        "SCHOOL", "COUNTRY",
    ]

    # if info_df["DRAFT_YEAR"] != "Undrafted":
    #     cols2keep += ["DRAFT_YEAR", "DRAFT_ROUND", "DRAFT_NUMBER"]

    return info_df[cols2keep].iloc[0]


def convert_to_cm(height_str) -> int:
    feet, inches = height_str.split('-')
    total_inches = int(feet) * 12 + int(inches)
    return round(total_inches * 2.54)

def convert_to_kg(weight_lbs: int) -> int:
    return round(weight_lbs * 0.453592)


def fetch_players(season_id):
    print(f"Trying to fetch players for season {season_id}.")
    try: 
        player_ids = fetch_player_ids(season_id)
    except Exception as e:
        print(f"Couldn't fetch player_ids for season {season_id}: {e}")
        return
    
    print("Fetching info about players...")
    players_data = []
    for player_id in tqdm(player_ids):
        try: 
            info_df = fetch_player_info(player_id)
            sleep(0.35)
        except Exception as e: 
            print(f"Couldn't fetch info for player {player_id}: {e}")
            continue
        
        player_info = {
            "id": int(player_id), 
            "first_name": info_df["FIRST_NAME"],
            "last_name": info_df["LAST_NAME"],
            "full_name": info_df["FIRST_NAME"] + info_df["LAST_NAME"],
            "birthdate": pd.to_datetime(info_df["BIRTHDATE"]).strftime('%Y-%m-%d'),
            "height": convert_to_cm(info_df["HEIGHT"]),
            "weigth": convert_to_kg(int(info_df["WEIGHT"])),
            "position": info_df["POSITION"],
            "school": info_df["SCHOOL"],
            "country": info_df["COUNTRY"]
        }
        players_data.append(player_info)

    return players_data



MERGE_PLAYERS_QUERY = """
    UNWIND $players AS player 
    MERGE (p:Player {id: player.id})
    ON CREATE SET
        p.first_name = player.first_name,
        p.last_name = player.last_name,
        p.birth_date = date(player.birthdate),
        p.height = player.height,
        p.weigth = player.weigth,
        p.position = player.position,
        p.school = player.school,
        p.country = player.country
"""


def get_players(season_id):
    driver = get_driver()
    if not driver:
        return 

    try: 
        players_data = fetch_players(season_id)
    except Exception as e:
        print(f"Couldn't fetch players for season {season_id}: {e}")
        return None

    with driver.session() as session:
        print("Writing to db...")
        tx_fn = lambda tx: tx.run(MERGE_PLAYERS_QUERY, players=players_data)
        result = session.execute_write(tx_fn)
        print("Done.")

    driver.close()