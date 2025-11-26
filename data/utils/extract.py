import pandas as pd
from typing import List, Dict


def extract_periods(pbp: pd.DataFrame) -> List[Dict]:
    periods_mask = (pbp["actionType"] == "period")
    periods_df = pbp.loc[periods_mask, ["timeActual", "period"]]

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


def extract_subs(pbp: pd.DataFrame) -> pd.DataFrame:
    mask = (pbp["actionType"] == "substitution")
    return pbp.loc[mask, ["timeActual", "period", "clock", "subType", "personId", "teamId"]]


def extract_starters(boxscore: pd.DataFrame) -> pd.DataFrame: 
    mask = (boxscore["START_POSITION"] != "")
    return boxscore.loc[mask, ["PLAYER_ID", "TEAM_ID"]]


def extract_lineups(starters: List[int], subs: pd.DataFrame) -> List[Dict]:
    assert len(starters) == 5, f"Starters list must contain 5 players, but got {len(starters)}"
    
    lineup_history = [
        {
            "period": 1,
            "time": "",
            "clock": "PT12M00S",
            "ids": sorted(starters)
        }
    ]

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
                "period": int(group["period"].iloc[0]), 
                "time": time,
                "clock": group["clock"].iloc[0],
                "ids": sorted(list(current_lineup))
            }
            lineup_history.append(new_lineup_entry)

    return lineup_history