import pandas as pd
from pathlib import Path
from nba_api.stats.endpoints import ScheduleLeagueV2


def get_schedule(season_id):
    schedule = ScheduleLeagueV2(season=season_id)
    schedule_df = schedule.get_data_frames()[0]
    df = pd.DataFrame()
    df["datetime"] = schedule_df["gameDateTimeUTC"].astype("string")
    df["game_id"] = pd.to_numeric(schedule_df["gameId"], downcast='unsigned')
    df["home_team_id"] = pd.to_numeric(schedule_df["homeTeam_teamId"], downcast='unsigned')
    df["away_team_id"] = pd.to_numeric(schedule_df["awayTeam_teamId"], downcast='unsigned')
    return df


def save_schedule(season_id):
    schedule_df = None
    try: 
        schedule_df = get_schedule(season_id)
    except Exception as e:
        print(f"Some erros occured while trying to fetch the schedule for the {season_id} season: {e}")

    if schedule_df is not None and not schedule_df.empty:
        print(f"Successfully load data for the {season_id} season.")

        filename = Path(f"data/rs{season_id}/schedule.json")            
        try:
            filename.parent.mkdir(parents=True, exist_ok=True)
            schedule_df.to_json(filename, orient="records", indent=4)

        except OSError as e:
            print(f"Error creating directory {filename.parent}: {e}")

        except IOError as e:
            print(f"Error saving file {filename}: {e}")

        print(f"Successfully saved data to {filename}.")


def main():
    print("Starting to load the regular season schedules...")
    seasons = ["2022-23", "2023-24", "2024-25", "2025-26"]
    for season_id in seasons:
        save_schedule(season_id)


if __name__ == "__main__":
    main()
