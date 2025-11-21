import json  
from tqdm import tqdm
from time import sleep
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamDetails


def load_team_data():
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


def main():
    team_data = load_team_data()

    if team_data:
        filename = "data/teams.json"
        print(f"Saving data to {filename}...")

        try:
            with open(filename, 'w') as f:
                json.dump(team_data, f, indent=4)
            print(f"Successfully saved data to {filename}.")

        except IOError as e:
            print(f"Error saving file: {e}")

    else:
        print("No data was fetched or an error occurred. Exiting without saving.")

        
if __name__ == "__main__":
    main()