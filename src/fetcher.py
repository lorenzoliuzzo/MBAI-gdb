import pandas as pd
from typing import List, Dict
from time import sleep

from nba_api.stats.static import teams
from nba_api.stats.endpoints import \
    TeamDetails, \
    CommonAllPlayers, CommonPlayerInfo, \
    ScheduleLeagueV2, \
    BoxScoreTraditionalV2

from nba_api.live.nba.endpoints import PlayByPlay



def fetch_teams():
    print("Fetching all teams from NBA_API...")
    all_teams = teams.get_teams()
    n_teams = len(all_teams)
    print(f"Got {n_teams} teams. Now fetching arena for each...")

    team_data_list = []
    for team in all_teams:
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



def fetch_schedule(season_id) -> List[Dict]:
    print(f"Fetching games schedule for season {season_id} from NBA_API...")
    schedule = ScheduleLeagueV2(season=season_id)
    schedule_df = schedule.get_data_frames()[0]
    df = pd.DataFrame()
    df["datetime"] = schedule_df["gameDateTimeUTC"].astype("string")
    df["game_id"] = pd.to_numeric(schedule_df["gameId"], downcast="unsigned")
    df["home_team_id"] = pd.to_numeric(schedule_df["homeTeam_teamId"], downcast="unsigned")
    df["away_team_id"] = pd.to_numeric(schedule_df["awayTeam_teamId"], downcast="unsigned")
    return df.to_dict("records")



def fetch_boxscore(game_id: int) -> pd.DataFrame:
    data = None
    try:
        boxscore = BoxScoreTraditionalV2(game_id=f"00{game_id}")
        data = boxscore.get_data_frames()[0]
    except Exception as e:
        print(f": {e}.")
    finally:
        return data
    


def fetch_pbp(game_id: int) -> pd.DataFrame:
    pbp = PlayByPlay(game_id=f"00{game_id}").get_dict()
    df = pd.DataFrame(pbp["game"]["actions"])

    id_cols = df.filter(regex="Id$").columns
    df[id_cols] = df[id_cols].astype("UInt32")
    df["timeActual"] = pd.to_datetime(df["timeActual"])
    df["period"] = df["period"].astype("uint8")
    df["actionType"] = df["actionType"].astype("string") 
    df["subType"] = df["subType"].astype("string") 
    df["descriptor"] = df["descriptor"].astype("string") 
    df["x"] = df["x"].astype("float16")
    df["y"] = df["y"].astype("float16")
    df["shotDistance"] = df["shotDistance"].astype("float16")

    return df.sort_values(by="timeActual", ascending=True).fillna(-1, axis=1)