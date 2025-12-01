import pandas as pd
from typing import List, Dict


def extract_periods(pbp: pd.DataFrame) -> List[Dict]:
    periods_mask = (pbp["actionType"] == "period")
    periods_df = pbp.loc[periods_mask, ["timeActual", "period"]]

    periods = []
    for period, group in periods_df.groupby("period", sort=False):
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


def extract_lineups(subs: pd.DataFrame, starters: List[int]) -> List[Dict]:
    assert len(starters) == 5, f"Starters list must contain 5 players, but got {len(starters)}"

    history = []
    current_lineup = set(starters)
    max_period = max(subs['period'].max(), 4) if not subs.empty else 4    
    for period in range(1, int(max_period) + 1):       
        start_clock = "PT05M00.00S" if period > 4 else "PT12M00.00S"
        period_subs = subs[subs['period'] == period]
        
        start_subs = period_subs[period_subs['clock'] == start_clock]
        for _, row in start_subs.iterrows():
            if row["subType"] == "in":
                current_lineup.add(row["personId"])
            elif row["subType"] == "out":
                current_lineup.discard(row["personId"])
                
        starters_entry = {
            "period": period,
            "time": "",
            "clock": start_clock,
            "ids": sorted(list(current_lineup))
        }
        history.append(starters_entry)

        regular_subs = period_subs[period_subs['clock'] != start_clock]
        for clock, group in regular_subs.groupby("clock", sort=False):
            for _, row in group.iterrows():
                if row["subType"] == "in":
                    current_lineup.add(row["personId"])
                elif row["subType"] == "out":
                    current_lineup.discard(row["personId"])
            
            if len(current_lineup) == 5:
                if current_lineup != set(history[-1]["ids"]):
                    history.append({
                        "period": period, 
                        "time": group['timeActual'].iloc[0],
                        "clock": clock,
                        "ids": sorted(list(current_lineup))
                    })
        

    return history