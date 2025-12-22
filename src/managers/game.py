from typing import List, Tuple, Optional
import pandas as pd
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError, CypherTypeError

from ..manager import BaseManager

from ..fetcher import fetch_boxscore, fetch_pbp
from ..queries.game import GET_TEAMS, MERGE_PERIODS, MERGE_STINTS, MERGE_SHOTS


class GameManager(BaseManager):

    def __init__(self, game_id: int):
        super().__init__()
        self.game_id = game_id

        try: 
            params = {"game_id": game_id}
            result = self.execute_read(GET_TEAMS, params)

            if not result:
                raise ValueError(f"Game {game_id} not found in database!")
            
            self.team_ids = (result['home_team_id'], result['away_team_id'])
            # self.home_team_id = result['home_team_id']
            # self.away_team_id = result['away_team_id']

        except ValueError as e:
            print(f"⚠️ Data Warning in `get_teams`: {e}")
            raise 

        except (ServiceUnavailable, CypherSyntaxError, CypherTypeError) as e:
            print(f"❌ Database Error in `get_teams`: {e}")
            raise

        except Exception as e: 
            print(f"❌ Unexpected Error in `get_teams`: {e}")
            raise


    def load_game(self) -> None:

        ht_id, at_id = self.team_ids
        print(f"🏀 Loading game {self.game_id} (Home: {ht_id} vs Away: {at_id})...")       

        try: 
            boxscore_df = fetch_boxscore(self.game_id)
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {self.game_id}: couldn't fetch the boxscore: {e}")
            return None


        try: 
            pbp_df = fetch_pbp(self.game_id)
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {self.game_id}: couldn't fetch the play-by-play actions: {e}")
            return None


        try: 
            self.load_periods( 
                periods = pbp_df.loc[pbp_df["actionType"] == "period", 
                    ["timeActual", "period"]
                ]
            )
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {self.game_id}: couldn't load periods: {e}")
            return None


        try:             
            self.load_lineups(
                subs = pbp_df.loc[pbp_df["actionType"] == "substitution", 
                    ["timeActual", "period", "clock", "subType", "personId", "teamId"]
                ], 
                starters = boxscore_df.loc[boxscore_df["START_POSITION"] != "", 
                    ["PLAYER_ID", "TEAM_ID"]
                ]
            )
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {self.game_id}: couldn't load lineups: {e}")
            return None


        try:             
            self.load_actions(
                actions = pbp_df.loc[pbp_df["actionType"] != "substitution",
                    [
                        "timeActual", "period", "clock", 
                        "actionType", "subType", 
                        "descriptor",
                        "x", "y",
                        "shotDistance", "shotResult", 
                        "teamId", "personId",
                        "jumpBallRecoverdPersonId",
                        "jumpBallWonPersonId",
                        "jumpBallLostPersonId",
                        "assistPersonId",
                        "blockPersonId",
                        "stealPersonId",
                        "foulDrawnPersonId",
                        "officialId"
                    ]
                ]
            )
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {self.game_id}: couldn't load actions: {e}")
            return None



    def load_periods(self, periods: pd.DataFrame) -> None:

        data = []
        for p, period_df in periods.groupby("period", sort=False):
            times = pd.to_datetime(period_df["timeActual"])
            period_entry = {"n": int(p), "start": times.iloc[0], "end": times.iloc[1]}
            data.append(period_entry)

        params = {"game_id": self.game_id, "periods": data}
        result = self.execute_write(MERGE_PERIODS, params)



    def load_lineups(self, subs: pd.DataFrame, starters: pd.DataFrame) -> None:

        data = []
        for team_id in self.team_ids:
            team_subs = subs[subs['teamId'] == team_id]
            team_starters = starters.loc[starters['TEAM_ID'] == team_id, 'PLAYER_ID'].to_list()
            assert len(team_starters) == 5, f"Starters list must contain 5 players, but got {len(starters)}"
            
            team_lineups = []
            current_lineup = set(team_starters)
            for p, period_df in team_subs.groupby('period', sort=False):
                start_clock = "PT05M00.00S" if p > 4 else "PT12M00.00S"
                start_mask = (period_df['clock'] == start_clock)    

                for _, row in period_df[start_mask].iterrows():
                    player = row["personId"]
                    if row["subType"] == "in":
                        current_lineup.add(player)
                    else:
                        current_lineup.discard(player)
                        
                starters_entry = {
                    "period": p, "time": "", "clock": start_clock,
                    "ids": sorted(list(current_lineup))
                }
                team_lineups.append(starters_entry)

                for clock, group in period_df[~start_mask].groupby("clock", sort=False):
                    time = group['timeActual'].iloc[0]
                    for _, row in group.iterrows():
                        player = row["personId"]    
                        if row["subType"] == "in":
                            current_lineup.add(player)
                        else:
                            current_lineup.discard(player)
                    
                    if len(current_lineup) == 5:
                        if current_lineup != set(team_lineups[-1]["ids"]):
                            lineup_entry = {
                                "period": p, "clock": clock, "time": time,
                                "ids": sorted(list(current_lineup))
                            }
                            team_lineups.append(lineup_entry)
        
            side_entry = {"team_id": team_id, "lineups": team_lineups}
            data.append(side_entry)

        params = {"game_id": self.game_id, "sides": data}
        result = self.execute_write(MERGE_STINTS, params)



    def load_actions(self, actions: pd.DataFrame) -> None:
        action_cols = ["actionType", "subType"]
        time_cols = ["timeActual", "period", "clock"]
        id_cols = ["teamId", "personId", "assistPersonId", "blockPersonId"]
        shot_cols = ["x", "y", "shotDistance", "shotResult"]

        mask_2pt = actions["actionType"] == "2pt"
        mask_3pt = actions["actionType"] == "3pt"

        self.load_shots(
            shots = actions.loc[mask_2pt | mask_3pt,
                action_cols + time_cols + id_cols + shot_cols
            ]
        )

    
    def load_shots(self, shots: pd.DataFrame) -> None:
        data = []
        for _, shot in shots.fillna(-1, axis=1).iterrows():
            shot_entry = {
                "type": shot["actionType"],
                "subtype": shot["subType"],
                "time": pd.to_datetime(shot["timeActual"]),
                "period": int(shot["period"]),
                "clock": shot["clock"],
                "team_id": int(shot["teamId"]) if shot["teamId"] != -1 else None,
                "player_id": int(shot["personId"]) if shot["personId"] != -1 else None,
                "result": shot["shotResult"],
                "x": float(shot["x"]) if shot["x"] != -1 else None,
                "y": float(shot["y"]) if shot["y"] != -1 else None,
                "distance": float(shot["shotDistance"]) if shot["shotDistance"] != -1 else None, 
                "assist_id": int(shot["assistPersonId"]) if shot["assistPersonId"] != -1 else None,
                "block_id": int(shot["blockPersonId"]) if shot["blockPersonId"] != -1 else None,   
            }

            data.append(shot_entry)

        params = {"game_id": self.game_id, "shots": data}
        self.execute_write(MERGE_SHOTS, params)
