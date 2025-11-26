import pandas as pd
from typing import List


def extract_periods(periods_df):
    periods = []
    for period, group in periods_df.groupby("period"):
        times = pd.to_datetime(group["timeActual"])
        p = {
            "n": int(period), 
            "start": times.iloc[0], 
            "end": times.iloc[1]
        }
        periods.append(p)
    return periods


def extract_starters(boxscore: pd.DataFrame) -> pd.DataFrame: 
    starters_mask = (boxscore["START_POSITION"] != "")
    return boxscore.loc[starters_mask, ["PLAYER_ID", "TEAM_ID"]]


def extract_lineups(starters: List[int], subs: pd.DataFrame):
    assert len(starters) == 5, f"Starters list must contain 5 players, but got {len(starters)}"
    starters_entry = {
        "time": "",
        "period": 1,
        "clock": "PT12M00S",
        "ids": sorted(starters)
    }
    lineup_history = [starters_entry]

    current_lineup = set(starters)
    for time, group in subs.groupby("timeActual"):
        for _, row in group.iterrows():
            player = row["personId"]
            if row["subType"] == "in":
                current_lineup.add(player)
            else:
                current_lineup.discard(player)
       
        if len(current_lineup) == 5:            
            new_lineup_entry = {
                "time": time,
                "period": int(group["period"].iloc[0]), 
                "clock": group["clock"].iloc[0],
                "ids": sorted(list(current_lineup))
            }
            lineup_history.append(new_lineup_entry)
        # else:
        #     print("!")

    return lineup_history

