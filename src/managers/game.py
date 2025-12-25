from typing import Tuple, List, Dict, Optional, Any
import pandas as pd
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError, CypherTypeError

from ..manager import BaseManager

from ..fetcher import fetch_boxscore, fetch_pbp
from ..queries.game import \
    GET_TEAMS, \
    MERGE_PERIODS, MERGE_STINTS, \
    MERGE_JUMPBALLS, MERGE_VIOLATIONS, MERGE_FOULS, \
    MERGE_SHOTS, MERGE_FREETHROWS, \
    MERGE_REBOUNDS, MERGE_TURNOVERS, MERGE_TIMEOUTS, \
    MERGE_SCORES, SET_PLUS_MINUS


import torch
from torch_geometric.data import HeteroData

class GameManager(BaseManager):

    def __init__(self, game_id: int):
        super().__init__()
        self.game_id = game_id

        try: 
            params = {"game_id": game_id}
            result = self.execute_read(GET_TEAMS, params)[0]

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
                period_len = 300.0 if p > 4 else 720.0                
                global_offset = 2880.0 + (p - 5) * 300.0 if p > 4 else (p - 1) * 720.0    
                start_clock = "PT05M00.00S" if p > 4 else "PT12M00.00S"
                start_mask = (period_df['clock'] == start_clock)    

                for _, row in period_df[start_mask].iterrows():
                    player = row["personId"]
                    if row["subType"] == "in":
                        current_lineup.add(player)
                    else:
                        current_lineup.discard(player)
                        
                starters_entry = {
                    "period": p, 
                    "time": "", 
                    "clock": start_clock,
                    "local_clock": 0.0,
                    "global_clock": global_offset, 
                    "ids": sorted(list(current_lineup))
                }
                team_lineups.append(starters_entry)

                for clock, group in period_df[~start_mask].groupby("clock", sort=False):
                    period_elapsed = period_len - pd.Timedelta(clock).total_seconds()   

                    for _, row in group.iterrows():
                        player = row["personId"]    
                        if row["subType"] == "in":
                            current_lineup.add(player)
                        else:
                            current_lineup.discard(player)
                    
                    if len(current_lineup) == 5:
                        if current_lineup != set(team_lineups[-1]["ids"]):
                            lineup_entry = {
                                "period": p, 
                                "time": group['timeActual'].iloc[0],
                                "clock": clock,
                                "local_clock": period_elapsed,
                                "global_clock": global_offset + period_elapsed, 
                                "ids": sorted(list(current_lineup))
                            }
                            team_lineups.append(lineup_entry)
        
            side_entry = {"team_id": team_id, "lineups": team_lineups}
            data.append(side_entry)

        params = {"game_id": self.game_id, "sides": data}
        result = self.execute_write(MERGE_STINTS, params)



    def load_actions(self, actions: pd.DataFrame) -> None:

        def process_action(row) -> Dict[str, Any]:
            remaining = pd.Timedelta(row["clock"]).total_seconds()         
            if row["period"] <= 4:
                period_elapsed = 720.0 - remaining
                global_offset = (row["period"] - 1) * 720.0
            else:
                period_elapsed = 300.0 - remaining
                global_offset = 2880.0 + (row["period"] - 5) * 300.0
            
            return {
                "time": row["timeActual"],
                "period": row["period"],
                "clock": row["clock"],
                "local_clock": round(period_elapsed, 2),
                "global_clock": round(global_offset + period_elapsed, 2),
                "type": row["actionType"],
                "subtype": row["subType"],
                "team_id": row["teamId"] if row["teamId"] != -1 else None,
                "player_id": row["personId"] if row["personId"] != -1 else None
            }

        def process_jumpball(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({
                "descriptor": row["descriptor"] if row["descriptor"] != -1 else None, 
                "recovered_id": row["jumpBallRecoverdPersonId"] if row["jumpBallRecoverdPersonId"] != -1 else None,
                "won_id": row["jumpBallWonPersonId"] if row["jumpBallWonPersonId"] != -1 else None,
                "lost_id": row["jumpBallLostPersonId"] if row["jumpBallLostPersonId"] != -1 else None
            })
            return entry

        def process_violation(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({
                "official_id": row["officialId"] if row["officialId"] != -1 else None
            })
            return entry
                
        def process_foul(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({
                "descriptor": row["descriptor"] if row["descriptor"] != -1 else None, 
                "drawn_id": row["foulDrawnPersonId"] if row["foulDrawnPersonId"] != -1 else None,
                "official_id": row["officialId"] if row["officialId"] != -1 else None
            })
            return entry
                
        def process_shot(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({
                "result": row["shotResult"],
                "x": row["x"], "y": row["y"],
                "distance": row["shotDistance"],
                "descriptor": row["descriptor"] if row["descriptor"] != -1 else None, 
                "assist_id": row["assistPersonId"] if row["assistPersonId"] != -1 else None,
                "block_id": row["blockPersonId"] if row["blockPersonId"] != -1 else None,   
            })
            return entry
        
        def process_freethrow(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({"result": row["shotResult"]})
            return entry

        def process_turnover(row) -> Dict[str, Any]:
            entry = process_action(row)
            entry.update({
                "descriptor": row["descriptor"] if row["descriptor"] != -1 else None, 
                "steal_id": row["stealPersonId"] if row["stealPersonId"] != -1 else None, 
                "official_id": row["officialId"] if row["officialId"] != -1 else None
            })
            return entry
        

        jumpball_mask = actions["actionType"] == "jumpball"
        jumpball_data = actions[jumpball_mask].apply(process_jumpball, axis=1)
        jumpball_params = {"game_id": self.game_id, "jumpballs": jumpball_data.tolist()}
        self.execute_write(MERGE_JUMPBALLS, jumpball_params)

        violation_mask = actions["actionType"] == "violation"
        violation_data = actions[violation_mask].apply(process_violation, axis=1)
        violation_params = {"game_id": self.game_id, "violations": violation_data.tolist()}
        self.execute_write(MERGE_VIOLATIONS, violation_params)

        foul_mask = actions["actionType"] == "foul"
        foul_data = actions[foul_mask].apply(process_foul, axis=1)
        foul_params = {"game_id": self.game_id, "fouls": foul_data.tolist()}
        self.execute_write(MERGE_FOULS, foul_params)

        shot_mask = (actions["actionType"] == "2pt") | (actions["actionType"] == "3pt")
        shot_data = actions[shot_mask].apply(process_shot, axis=1)
        shot_params = {"game_id": self.game_id, "shots": shot_data.tolist()}
        self.execute_write(MERGE_SHOTS, shot_params)

        ft_mask = actions["actionType"] == "freethrow"
        ft_data = actions[ft_mask].apply(process_freethrow, axis=1)
        ft_params = {"game_id": self.game_id, "shots": ft_data.tolist()}
        self.execute_write(MERGE_FREETHROWS, ft_params)

        reb_mask = actions["actionType"] == "rebound"
        reb_data = actions[reb_mask].apply(process_action, axis=1)
        reb_params = {"game_id": self.game_id, "rebounds": reb_data.tolist()}
        self.execute_write(MERGE_REBOUNDS, reb_params)

        tov_mask = actions["actionType"] == "turnover"
        tov_data = actions[tov_mask].apply(process_turnover, axis=1)
        tov_params = {"game_id": self.game_id, "turnovers": tov_data.tolist()}
        self.execute_write(MERGE_TURNOVERS, tov_params)

        timeout_mask = actions["actionType"] == "timeout"
        timeout_data = actions[timeout_mask].apply(process_action, axis=1)
        timeout_params = {"game_id": self.game_id, "timeouts": timeout_data.tolist()}
        self.execute_write(MERGE_TIMEOUTS, timeout_params)

        params = {"game_id": self.game_id}
        self.execute_write(MERGE_SCORES, params)
        self.execute_write(SET_PLUS_MINUS, params)


    
    def to_pyg(self) -> HeteroData:
        data = HeteroData()

        data['game'].x = torch.tensor([[1.0]], dtype=torch.float)
        data['team'].x = torch.eye(2)

        data['team', 'played_home', 'game'].edge_index = torch.tensor([[0], [0]], dtype=torch.long)
        data['team', 'played_away', 'game'].edge_index = torch.tensor([[1], [0]], dtype=torch.long)

        l_uids = []; p_ids = []; ls_uids = []; ps_uids = []
        l_seen = set(); p_seen = set(); ls_seen = set(); ps_seen = set()
        
        t_l_edges = set(); p_l_edges = set(); 
        l_ls_edges = set(); p_ps_edges = set()
        ps_ls_edges = set()

        ls_clocks = {}; ps_clocks = {}; ls_durations = {}; ps_durations = {}

        query = """
            MATCH (g:Game {id: $game_id})
            MATCH (t:Team)-[:HAS_LINEUP]->(l:LineUp)-[:ON_COURT]->(ls:LineUpStint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
            MATCH (l)<-[:MEMBER_OF]-(p:Player)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
            RETURN 
                t.id AS t_id, 
                elementId(l) AS l_id,
                p.id AS p_id,
                elementId(ls) AS ls_id, ls.global_clock AS ls_clock, ls.clock_duration AS ls_duration,
                elementId(ps) AS ps_id, ps.global_clock AS ps_clock, ps.clock_duration AS ps_duration
            ORDER BY ps_clock ASC
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for row in results:
            if row['l_id'] not in l_seen:
                l_uids.append(row['l_id'])
                l_seen.add(row['l_id'])
        
            if row['p_id'] not in p_seen:
                p_ids.append(row['p_id'])
                p_seen.add(row['p_id'])

            if row['ls_id'] not in ls_seen:
                ls_uids.append(row['ls_id'])
                ls_seen.add(row['ls_id'])
                ls_clocks[row['ls_id']] = row['ls_clock'] 
                ls_durations[row['ls_id']] = row['ls_duration'] 

            if row['ps_id'] not in ps_seen:
                ps_uids.append(row['ps_id'])
                ps_seen.add(row['ps_id'])
                ps_clocks[row['ps_id']] = row['ps_clock'] 
                ps_durations[row['ps_id']] = row['ps_duration'] 

        g_map = {self.game_id: 0}
        t_map = {self.team_ids[0]: 0, self.team_ids[1]: 1}
        l_map = {uid: i for i, uid in enumerate(l_uids)}        
        p_map = {id: i for i, id in enumerate(p_ids)}
        ls_map = {uid: i for i, uid in enumerate(ls_uids)}        
        ps_map = {uid: i for i, uid in enumerate(ps_uids)}        

        data['lineup'].x = torch.ones((len(l_uids), 1), dtype=torch.float)        
        data['player'].x = torch.ones((len(p_ids), 1), dtype=torch.float)
        
        data['lineup_stint'].x = torch.tensor(
            [[ls_clocks[uid], ls_durations[uid]] for uid in ls_uids], 
            dtype=torch.float
        )
        
        data['player_stint'].x = torch.tensor(
            [[ps_clocks[uid], ps_durations[uid]] for uid in ps_uids], 
            dtype=torch.float
        )
        
        for row in results:
            # TEAM - HAS_LINEUP - LINEUP
            if row['t_id'] in t_map and row['l_id'] in l_map:
                t_l_edges.add((t_map[row['t_id']], l_map[row['l_id']]))

            # PLAYER - MEMBER_OF - LINEUP
            if row['p_id'] in p_map and row['l_id'] in l_map:
                p_l_edges.add((p_map[row['p_id']], l_map[row['l_id']]))
    
            # LINEUP - ON_COURT - LINEUPSTINT
            if row['l_id'] in l_map and row['ls_id'] in ls_map:
                l_ls_edges.add((l_map[row['l_id']], ls_map[row['ls_id']]))

            # PLAYER - ON_COURT - PLAYERSTINT
            if row['p_id'] in p_map and row['ps_id'] in ps_map:
                p_ps_edges.add((p_map[row['p_id']], ps_map[row['ps_id']]))

            # PLAYERSTINT - ON_COURT_WITH - LINEUPSTINT
            if row['ps_id'] in ps_map and row['ls_id'] in ls_map:
                ps_ls_edges.add((ps_map[row['ps_id']], ls_map[row['ls_id']]))

        t_l_edges_tensor = torch.tensor(list(t_l_edges), dtype=torch.long).t().contiguous()
        data['team', 'has_lineup', 'lineup'].edge_index = t_l_edges_tensor

        p_l_edges_tensor = torch.tensor(list(p_l_edges), dtype=torch.long).t().contiguous()
        data['player', 'member_of', 'lineup'].edge_index = p_l_edges_tensor
        
        l_ls_edges_tensor = torch.tensor(list(l_ls_edges), dtype=torch.long).t().contiguous()            
        data['lineup', 'on_court', 'lineup_stint'].edge_index = l_ls_edges_tensor
        
        p_ps_edges_tensor = torch.tensor(list(p_ps_edges), dtype=torch.long).t().contiguous()
        data['player', 'on_court', 'player_stint'].edge_index = p_ps_edges_tensor
        
        ps_ls_edges_tensor = torch.tensor(list(ps_ls_edges), dtype=torch.long).t().contiguous()
        data['player_stint', 'on_court_with', 'player_stint'].edge_index = ps_ls_edges_tensor

        return data