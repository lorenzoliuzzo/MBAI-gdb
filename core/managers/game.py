# core/managers/game.py

from typing import List, Dict, Tuple, Optional
import pandas as pd
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError, CypherTypeError

from ..manager import BaseManager

from ..fetcher import fetch_boxscore, fetch_pbp
from ..queries.game import GET_TEAMS, MERGE_PERIODS, MERGE_STINTS


class GameManager(BaseManager):


    def get_teams(self, 
            game_id: int
        ) -> Optional[Tuple[int, int]]:

        try: 
            params = {"game_id": game_id}
            result = self.execute_read(GET_TEAMS, params)

            if not result:
                raise ValueError(f"game {game_id} not found in database!")
                
            return result['home_team_id'], result['away_team_id']

        except ValueError as e:
            print(f"⚠️ Data Warning in `get_teams`: {e}")
            return None 

        except (ServiceUnavailable, CypherSyntaxError, CypherTypeError) as e:
            print(f"❌ Database Error in `get_teams`: {e}")
            raise e

        except Exception as e: 
            print(f"❌ Unexpected Error in `get_teams`: {e}")
            return None



    def load_game(self, 
            game_id: int
        ) -> None:

        try: 
            teams = self.get_teams(game_id)
            if teams is None:
                print(f"⏭️ Skipping game {game_id}: couldn't resolve teams.")
                return None

            ht_id, at_id = teams
            print(f"🏀 Loading game {game_id} (Home: {ht_id} vs Away: {at_id})...")
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {game_id}: {e}")
            return None
        

        try: 
            boxscore_df = fetch_boxscore(game_id)
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {game_id}: couldn't fetch the boxscore: {e}")
            return None


        try: 
            pbp_df = fetch_pbp(game_id)
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {game_id}: couldn't fetch the play-by-play actions: {e}")
            return None


        try: 
            self.load_periods(game_id, 
                periods = pbp_df.loc[pbp_df["actionType"] == "period", 
                    ["timeActual", "period"]
                ]
            )
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {game_id}: couldn't load periods: {e}")
            return None


        try:             
            self.load_lineups(game_id, teams,
                subs = pbp_df.loc[pbp_df["actionType"] == "substitution", 
                    ["timeActual", "period", "clock", "subType", "personId", "teamId"]
                ], 
                starters = boxscore_df.loc[boxscore_df["START_POSITION"] != "", 
                    ["PLAYER_ID", "TEAM_ID"]
                ]
            )
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {game_id}: couldn't load lineups: {e}")
            return None



        try:             
            self.load_possessions(game_id, 
                
            )
        
        except Exception as e: 
            print(f"⛔ Critical failure in `load_game` for ID {game_id}: couldn't load possessions: {e}")
            return None



    def load_periods(self, 
            game_id: int, 
            periods: pd.DataFrame
        ) -> None:

        data = []
        for p, period_df in periods.groupby("period", sort=False):
            times = pd.to_datetime(period_df["timeActual"])
            period_entry = {"n": int(p), "start": times.iloc[0], "end": times.iloc[1]}
            data.append(period_entry)

        params = {"game_id": game_id, "periods": data}
        result = self.execute_write(MERGE_PERIODS, params)


        
    def load_lineups(self, 
            game_id: int, 
            teams: Tuple[int, int], 
            subs: pd.DataFrame, 
            starters: pd.DataFrame
        ) -> None:

        data = []
        for team_id in teams:
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

        params = {"game_id": game_id, "sides": data}
        result = self.execute_write(MERGE_STINTS, params)
