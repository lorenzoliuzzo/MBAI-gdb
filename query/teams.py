import json 
from tqdm import tqdm
from time import sleep
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamDetails

from driver import get_driver
from router import get_data_path


def fetch_teams():
    print("Fetching all teams from NBA_API...")
    all_teams = teams.get_teams()
    n_teams = len(all_teams)
    print(f"Got {n_teams} teams. Now fetching arena for each...")

    team_data_list = []
    for team in tqdm(all_teams):
        try:
            team_details = TeamDetails(team['id']).get_dict()
            background = team_details["resultSets"][0]
            background_dict = dict(zip(background["headers"], background["rowSet"][0]))
            arena = background_dict["ARENA"]

            team_data_list.append(
                {
                    'id': team['id'],
                    'full_name': team['full_name'],
                    'abbreviation': team['abbreviation'],
                    'city': team['city'],
                    'state': team['state'],
                    'arena': arena
                }
            )

            sleep(0.5)

        except Exception as e:
            print(f"Failed to fetch details for team {team['full_name']}: {e}")
            return None

    print("All team and arena data fetched.")
    return team_data_list


def save_teams():
    teams_data = fetch_teams()
    if not team_data:
        print("")
        return 

    filename = get_data_path() / "teams.json"
    print(f"Saving data to {filename}...")

    try:
        with open(filename, 'w') as f:
            json.dump(team_data, f, indent=4)
        print(f"Successfully saved data to {filename}.")

    except IOError as e:
        print(f"Error saving file: {e}")



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