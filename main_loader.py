import json
from driver import get_driver


def create_teams_tx(tx, teams):
    query = """
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
    result = tx.run(query, teams=teams)
    return result.single()[0]

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
            result = session.execute_write(create_teams_tx, teams_data_list)
            print(f"Successfully processed {result} teams.")

        driver.close()


def create_season_schedule_tx(tx, season_id, schedule):
    query = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game
    
    MATCH (ht:Team {id: game.home_team_id})
    MATCH (at:Team {id: game.away_team_id})
    
    MERGE (g:Game {id: game.game_id})
    ON CREATE SET
        g.datetime = datetime(game.datetime)
    
    MERGE (s)-[:HAS_GAME]->(g)
    MERGE (ht)-[:HOME_TEAM]->(g)
    MERGE (at)-[:AWAY_TEAM]->(g)
    
    RETURN count(g)
    """
    result = tx.run(query, season_id=season_id, schedule=schedule)
    return result.single()[0]

def create_season_schedule(season_id):
    try:
        with open(f"data/rs{season_id}/schedule.json", 'r') as f:
            schedule = json.load(f)

    except Exception as e:
        print(f"An unexpected error occurred while reading the schedule for season {season_id}: {e}")
        return None
        
    driver = get_driver()
    if driver:
        with driver.session() as session:
            result = session.execute_write(create_season_schedule_tx, season_id, schedule)
            print(f"Successfully processed {result} games for season {season_id}.")

        driver.close()