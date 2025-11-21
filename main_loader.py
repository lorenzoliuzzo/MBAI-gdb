import json
from driver import get_driver


def create_all_teams_tx(tx, teams):
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


def main():
    try:
        with open("data/teams.json", 'r') as f:
            team_data_list = json.load(f)

    except Exception as e:
        print(f"An unexpected error occurred while reading the teams data: {e}")
        return None
        
    driver = get_driver()
    if not driver:
        return

    with driver.session() as session:
        print("Writing all teams to database...")
        result = session.execute_write(create_all_teams_tx, team_data_list)
        print(f"Successfully processed {result} teams.")

    driver.close()
    
    
if __name__ == "__main__":
    main()