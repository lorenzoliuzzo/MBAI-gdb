from typing import Tuple, List, Dict, Optional, Any
import pandas as pd
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError, CypherTypeError

from ..manager import BaseManager

from ..fetcher import fetch_boxscore, fetch_pbp
from ..queries.game import \
    GET_TEAMS, \
    MERGE_PERIODS, MERGE_STINTS, \
    MERGE_SHOTS, MERGE_FREETHROWS, \
    MERGE_REBOUNDS, MERGE_FOULS, \
    MERGE_SCORES


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

        def process_action(row) -> Dict[str, Any]:
            return {
                "time": row["timeActual"],
                "period": row["period"],
                "clock": row["clock"],
                "type": row["actionType"],
                "subtype": row["subType"],
                "team_id": row["teamId"] if row["teamId"] != -1 else None,
                "player_id": row["personId"] if row["personId"] != -1 else None,
            }

        def process_foul(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({"drawn_id": row["foulDrawnPersonId"] if row["foulDrawnPersonId"] != -1 else None})
            return entry
                
        def process_shot(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({
                "result": row["shotResult"],
                "x": row["x"], "y": row["y"],
                "distance": row["shotDistance"],
                "assist_id": row["assistPersonId"] if row["assistPersonId"] != -1 else None,
                "block_id": row["blockPersonId"] if row["blockPersonId"] != -1 else None,   
            })
            return entry
        
        def process_freethrow(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({"result": row["shotResult"]})
            return entry


        foul_mask = actions["actionType"] == "foul"
        foul_data = actions[foul_mask].apply(process_foul, axis=1).tolist()
        foul_params = {"game_id": self.game_id, "fouls": foul_data}
        self.execute_write(MERGE_FOULS, foul_params)

        shot_mask = (actions["actionType"] == "2pt") | (actions["actionType"] == "3pt")
        shot_data = actions[shot_mask].apply(process_shot, axis=1).tolist()
        shot_params = {"game_id": self.game_id, "shots": shot_data}
        self.execute_write(MERGE_SHOTS, shot_params)

        ft_mask = actions["actionType"] == "freethrow"
        ft_data = actions[ft_mask].apply(process_freethrow, axis=1).tolist()
        ft_params = {"game_id": self.game_id, "shots": ft_data}
        self.execute_write(MERGE_FREETHROWS, ft_params)

        reb_mask = actions["actionType"] == "rebound"
        reb_data = actions[reb_mask].apply(process_action, axis=1).tolist()
        reb_params = {"game_id": self.game_id, "rebounds": reb_data}
        self.execute_write(MERGE_REBOUNDS, reb_params)

        self.execute_write(MERGE_SCORES, {"game_id": self.game_id})