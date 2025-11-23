import json 
import pandas as pd
from nba_api.stats.endpoints import ScheduleLeagueV2

from driver import get_driver
from router import get_season_path


MERGE_SCHEDULE_QUERY = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game

    MATCH (ht:Team {id: game.home_team_id})
    MATCH (at:Team {id: game.away_team_id})
    MATCH (a:Arena)<-[:PLAYS_IN]-(ht)

    MERGE (g:Game {id: game.game_id})
    ON CREATE SET
        g.date = datetime(game.datetime)

    MERGE (s)-[:HAS_GAME]-(g)
    MERGE (g)-[:AT]->(a)
    MERGE (ht)-[:PLAYS_HOME]->(g)
    MERGE (at)-[:PLAYS_AWAY]->(g)

    RETURN count(g)
"""

MERGE_NEXT_GAME_LINK_QUERY = """   
    MATCH (s:Season {id: $season_id})-[:HAS_GAME]->(g:Game)<-[:PLAYS_HOME|PLAYS_AWAY]-(t:Team)
    WITH t, g ORDER BY g.date ASC     
    
    WITH t, 
        collect(g) AS games, 
        size(collect(g)) AS list_size,
        range(0, size(collect(g)) - 2) AS indexes
    
    UNWIND indexes AS i        
    WITH t, 
        games[i] AS prev_g, 
        games[i+1] AS next_g,
        duration.between(games[i].date, games[i+1].date) AS delta_t

    MERGE (prev_g)-[:NEXT {time_since: delta_t}]->(next_g)

    RETURN count(prev_g) AS linked_games_count
"""


def fetch_schedule(season_id):
    schedule = ScheduleLeagueV2(season=season_id)
    schedule_df = schedule.get_data_frames()[0]
    df = pd.DataFrame()
    df["datetime"] = schedule_df["gameDateTimeUTC"].astype("string")
    df["game_id"] = pd.to_numeric(schedule_df["gameId"], downcast="unsigned")
    df["home_team_id"] = pd.to_numeric(schedule_df["homeTeam_teamId"], downcast="unsigned")
    df["away_team_id"] = pd.to_numeric(schedule_df["awayTeam_teamId"], downcast="unsigned")
    return df


def save_schedule(season_id):
    schedule_df = None
    try: 
        schedule_df = fetch_schedule(season_id)
    except Exception as e:
        print(f"Some erros occured while trying to fetch the {season_id} season schedule: {e}")

    if schedule_df is None and schedule_df.empty:
        return 

    print(f"Successfully fetched schedule data for the {season_id} season.")

    filename = get_season_path(season_id) / "schedule.json"
    try:
        filename.parent.mkdir(parents=True, exist_ok=True)
        schedule_df.to_json(filename, orient="records", indent=4)

    except OSError as e:
        print(f"Error creating directory {filename.parent}: {e}")

    except IOError as e:
        print(f"Error saving file {filename}: {e}")

    print(f"Successfully saved schedule data to {filename}.")


def create_schedule(season_id):
    filename = get_season_path(season_id) / "schedule.json"
    try:
        with open(filename, 'r') as f:
            schedule_data = json.load(f)

    except Exception as e:
        print(f"An unexpected error occurred while reading the schedule for season {season_id}: {e}")
        return None
        
    driver = get_driver()
    if driver:
        with driver.session() as session:
            print(f"Creating `Game`s for `Season` {season_id}...")

            tx_fn = lambda tx: tx.run(
                MERGE_SCHEDULE_QUERY, 
                season_id=season_id, 
                schedule=schedule_data
            )
            result = session.execute_write(tx_fn)
            print(f"Successfully created games.")
            
            tx_fn = lambda tx: tx.run(
                MERGE_NEXT_GAME_LINK_QUERY, 
                season_id=season_id
            )
            result = session.execute_write(tx_fn)
            print(f"Successfully linked games with `:NEXT'.")

        driver.close()