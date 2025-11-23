import json
import pandas as pd
from pathlib import Path
from datetime import datetime

from driver import get_driver


MERGE_TEAMS_QUERY = """
    UNWIND $teams AS team 
    MERGE (t:Team {id: team.id})
    ON CREATE SET
        t.name = team.full_name,
        t.abbreviation = team.abbreviation

    MERGE (a:Arena {name: team.arena})
    MERGE (t)-[:PLAYS_IN]->(a)

    MERGE (c:City {name: team.city})
    MERGE (a)-[:LOCATED_IN]->(c)

    MERGE (s:State {name: team.state})
    MERGE (c)-[:LOCATED_IN]->(s)

    RETURN count(t)
"""

def create_teams():
    try:
        with open("data/teams.json", 'r') as f:
            teams_data_list = json.load(f)

    except Exception as e:
        print(f"An unexpected error occurred while reading the teams data: {e}")
        return None
        
    driver = get_driver()
    if driver:
        with driver.session() as session:
            print("Writing all teams to database...")
            tx_fn = lambda tx: tx.run(MERGE_TEAMS_QUERY, teams=teams_data_list)
            result = session.execute_write(tx_fn).single()
            print(f"Successfully processed {result[0]} teams.")

        driver.close()



MERGE_SCHEDULE_QUERY = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game

    MATCH (ht:Team {id: game.home_team_id})-[:PLAYS_IN]->(a:Arena)
    MATCH (at:Team {id: game.away_team_id})

    MERGE (g:Game {id: game.game_id})
    ON CREATE SET
        g.date = datetime(game.datetime)

    MERGE (s)-[:HAS_GAME]->(g)-[:AT]->(a)
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


def create_schedule(season_id):
    filename = Path(f"data/rs{season_id}/schedule.json")
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



MERGE_PERIOD_QUERY = """
    MATCH (g:Game {id: $game_id})
    WITH g
    UNWIND $periods AS period

    MERGE (p:Period {n: period.n})
    ON CREATE SET
        p.start = datetime(period.start),
        p.end = datetime(period.end),
        p.duration = duration.between(period.start, period.end)

    MERGE (g)-[:HAS_PERIOD]->(p)

    WITH g, p, period
    WHERE period.n <= 4
    SET p:Regular
    REMOVE p:OverTime

    WITH g, p, period
    WHERE period.n > 4
    SET p:OverTime
    REMOVE p:Regular

    WITH p
    RETURN count(p) AS period_count
"""

MERGE_NEXT_PERIOD_LINK_QUERY = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, p
    ORDER BY p.n ASC
    
    MATCH (p_next:Period {n: p.n + 1})
    WHERE (g)-[:HAS_PERIOD]->(p_next)
    MERGE (p)-[:NEXT]->(p_next)
    RETURN count(p) AS linked_count
"""

SET_GAME_DURATION_QUERY = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, min(p.start) AS first_start, max(p.end) AS last_end
    SET 
        g.start = first_start,
        g.end = last_end,
        g.duration = duration.between(first_start, last_end)
"""
    

def create_game(season_id, game_id):
    filename = Path(f"data/rs{season_id}/games/g{game_id}.csv")
    game_df = None
    try: 
        game_df = pd.read_csv(filename)
    except Exception as e: 
        print(f"Some error occured while reading the game actions from {filename}: {e}")
    
    periods = []
    for n, period_df in game_df.groupby("period"):
        period_actions = period_df[period_df["actionType"] == "period"]
        start_action = period_actions[period_actions["subType"] == "start"]
        end_action = period_actions[period_actions["subType"] == "end"]
        start_time = pd.to_datetime(start_action["timeActual"]).item()
        end_time = pd.to_datetime(end_action["timeActual"]).item()
        p = {"n": n, "start": start_time, "end": end_time}
        periods.append(p)


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