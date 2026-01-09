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
    MERGE_NEXT_ACTION, MERGE_SCORES, SET_PLUS_MINUS

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
                        
                if len(current_lineup) == 5:
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
            entry.update({"official_id": row["officialId"] if row["officialId"] != -1 else None})
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
        self.execute_write(MERGE_NEXT_ACTION, params)
        self.execute_write(MERGE_SCORES, params)
        # self.execute_write(SET_PLUS_MINUS, params)


    
    def to_pyg(self) -> HeteroData:
        data = HeteroData()

        data['game'].x = torch.tensor([[1.0]], dtype=torch.float)
        data['team'].x = torch.eye(2)

        data['team', 'played_home', 'game'].edge_index = torch.tensor([[0], [0]], dtype=torch.long)
        data['team', 'played_away', 'game'].edge_index = torch.tensor([[1], [0]], dtype=torch.long)

        q_uids = []; l_uids = []; p_ids = []; ls_uids = []; ps_uids = []
        q_seen = set(); l_seen = set(); p_seen = set(); ls_seen = set(); ps_seen = set()
        
        q_g_edges = set(); 
        t_l_edges = set(); p_l_edges = set()
        l_ls_edges = set(); ls_q_edges = set()
        p_ps_edges = set(); ps_q_edges = set()
        ps_ls_edges = set()

        q_ns = {}
        ls_feats = {}
        ps_feats = {}

        query = """
            MATCH (g:Game {id: $game_id})<-[:IN_GAME]-(q:Period)
            MATCH (t:Team)-[:HAS_LINEUP]->(l:LineUp)-[:ON_COURT]->(ls:LineUpStint)-[:IN_PERIOD]->(q)
            MATCH (l)<-[:MEMBER_OF]-(p:Player)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
            RETURN 
                elementId(q) AS q_id, q.n AS q_n,
                t.id AS t_id, 
                elementId(l) AS l_id,
                p.id AS p_id,
                elementId(ls) AS ls_id, ls.global_clock AS ls_global_clock, ls.local_clock AS ls_local_clock, ls.clock_duration AS ls_duration,
                elementId(ps) AS ps_id, ps.global_clock AS ps_global_clock, ps.local_clock AS ps_local_clock, ps.clock_duration AS ps_duration
            ORDER BY ps_global_clock ASC
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for row in results:
            q = row['q_id']
            t = row['t_id']
            l = row['l_id']
            p = row['p_id']
            ls = row['ls_id']
            ps = row['ps_id']

            if q not in q_seen:
                q_uids.append(q)
                q_seen.add(q)
                q_ns[q] = row['q_n'] 

            if l not in l_seen:
                l_uids.append(l)
                l_seen.add(l)
        
            if p not in p_seen:
                p_ids.append(p)
                p_seen.add(p)

            if ls not in ls_seen:
                ls_uids.append(ls)
                ls_seen.add(ls)
                ls_feats[ls] = [row['ls_global_clock'], row['ls_local_clock'], row['ls_duration']]

            if ps not in ps_seen:
                ps_uids.append(ps)
                ps_seen.add(ps)
                ps_feats[ps] = [row['ps_global_clock'], row['ps_local_clock'], row['ps_duration']]

        g_map = {self.game_id: 0}
        t_map = {self.team_ids[0]: 0, self.team_ids[1]: 1}
        q_map = {uid: i for i, uid in enumerate(q_uids)}
        l_map = {uid: i for i, uid in enumerate(l_uids)}        
        p_map = {id: i for i, id in enumerate(p_ids)}
        ls_map = {uid: i for i, uid in enumerate(ls_uids)}        
        ps_map = {uid: i for i, uid in enumerate(ps_uids)}        

        data['period'].x = torch.tensor(
            [[q_ns[uid]] for uid in q_uids], 
            dtype=torch.float
        )

        data['lineup'].x = torch.ones((len(l_uids), 1), dtype=torch.float)        
        data['player'].x = torch.ones((len(p_ids), 1), dtype=torch.float)
        
        data['lineup_stint'].x = torch.tensor(
            [ls_feats[uid] for uid in ls_uids], 
            dtype=torch.float
        )
        
        data['player_stint'].x = torch.tensor(
            [ps_feats[uid] for uid in ps_uids], 
            dtype=torch.float
        )
        
        for row in results:
            q = row['q_id']
            t = row['t_id']
            l = row['l_id']
            p = row['p_id']
            ls = row['ls_id']
            ps = row['ps_id']

            q_g_edges.add((q_map[q], 0))
            t_l_edges.add((t_map[t], l_map[l]))
            p_l_edges.add((p_map[p], l_map[l]))   
            l_ls_edges.add((l_map[l], ls_map[ls]))
            p_ps_edges.add((p_map[p], ps_map[ps]))
            ps_ls_edges.add((ps_map[ps], ls_map[ls]))
            ls_q_edges.add((ls_map[ls], q_map[q]))
            ps_q_edges.add((ps_map[ps], q_map[q]))


        q_g_edges_tensor = torch.tensor(list(q_g_edges), dtype=torch.long).t().contiguous()
        data['period', 'in_game', 'game'].edge_index = q_g_edges_tensor

        t_l_edges_tensor = torch.tensor(list(t_l_edges), dtype=torch.long).t().contiguous()
        data['team', 'has_lineup', 'lineup'].edge_index = t_l_edges_tensor

        p_l_edges_tensor = torch.tensor(list(p_l_edges), dtype=torch.long).t().contiguous()
        data['player', 'member_of', 'lineup'].edge_index = p_l_edges_tensor
        
        l_ls_edges_tensor = torch.tensor(list(l_ls_edges), dtype=torch.long).t().contiguous()            
        data['lineup', 'on_court', 'lineup_stint'].edge_index = l_ls_edges_tensor

        p_ps_edges_tensor = torch.tensor(list(p_ps_edges), dtype=torch.long).t().contiguous()
        data['player', 'on_court', 'player_stint'].edge_index = p_ps_edges_tensor
        
        ps_ls_edges_tensor = torch.tensor(list(ps_ls_edges), dtype=torch.long).t().contiguous()
        data['player_stint', 'on_court_with', 'lineup_stint'].edge_index = ps_ls_edges_tensor

        ls_q_edges_tensor = torch.tensor(list(ls_q_edges), dtype=torch.long).t().contiguous()            
        data['lineup_stint', 'in_period', 'period'].edge_index = ls_q_edges_tensor

        ps_q_edges_tensor = torch.tensor(list(ps_q_edges), dtype=torch.long).t().contiguous()            
        data['player_stint', 'in_period', 'period'].edge_index = ps_q_edges_tensor


        ls_next_edges = []
        ps_next_edges = []

        query = """
            MATCH (g:Game {id: $game_id})
            
            MATCH (ls:LineUpStint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
            MATCH (ls)-[:NEXT]->(next_ls)
            RETURN 
                elementId(ls) as curr_id, 
                elementId(next_ls) as next_id, 
                'LineUpStint' as type
            
            UNION ALL

            MATCH (ps:PlayerStint)-[:ON_COURT_WITH]->(:LineUpStint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
            MATCH (ps)-[:NEXT]->(next_ps)
            RETURN 
                elementId(ps) as curr_id, 
                elementId(next_ps) as next_id, 
                'PlayerStint' as type
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for row in results:           
            if row['type'] == 'LineUpStint':
                if row['curr_id'] in ls_map and row['next_id'] in ls_map:
                    ls_next_edges.append([ls_map[row['curr_id'] ], ls_map[row['next_id']]])
            
            elif row['type'] == 'PlayerStint':
                if row['curr_id'] in ps_map and row['next_id'] in ps_map:
                    ps_next_edges.append([ps_map[row['curr_id'] ], ps_map[row['next_id']]])

        ls_next_tensor = torch.tensor(ls_next_edges, dtype=torch.long).t().contiguous()
        ps_next_tensor = torch.tensor(ps_next_edges, dtype=torch.long).t().contiguous()
        data['lineup_stint', 'next', 'lineup_stint'].edge_index = ls_next_tensor
        data['player_stint', 'next', 'player_stint'].edge_index = ps_next_tensor


        ocn_edges = []
        query = """
            MATCH (g:Game {id: $game_id})
            MATCH (ls1:LineUpStint)-[r:ON_COURT_NEXT]->(ls2:LineUpStint)
            WHERE (ls1)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
            RETURN elementId(ls1) as curr_id, elementId(ls2) as nxt_id
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for row in results:           
            if row['curr_id'] in ls_map and row['nxt_id'] in ls_map:
                ocn_edges.append([ls_map[row['curr_id']], ls_map[row['nxt_id']]])

        ocn_tensor = torch.tensor(ocn_edges, dtype=torch.long).t().contiguous()
        data['lineup_stint', 'on_court_next', 'lineup_stint'].edge_index = ocn_tensor


        foul_uids = []        
        foul_feats = []
        edge_foul = []      
        edge_foul_drawn = []    

        query = """
            MATCH (g:Game {id: $game_id})
            MATCH (ps:PlayerStint)-[:COMMITTED_FOUL]->(f:Foul)
            WHERE f.id STARTS WITH toString($game_id) 
            
            OPTIONAL MATCH (ps_v:PlayerStint)-[:DREW_FOUL]->(f)
            
            RETURN 
                elementId(f) AS foul_id,
                elementId(ps) AS player_id,
                elementId(ps_v) AS victim_id,
                labels(f) AS types,
                f.local_clock AS local_clock,
                f.global_clock AS global_clock
            ORDER BY global_clock ASC
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for i, row in enumerate(results): 
            foul_uids.append(row['foul_id'])
            foul_feats.append([
                row['global_clock'], row['local_clock'],   
            ])

            if row['player_id'] in ps_map:
                edge_foul.append([ps_map[row['player_id']], i])
            
            if row['victim_id'] and row['victim_id'] in ps_map:
                edge_foul_drawn.append([ps_map[row['victim_id']], i])

        foul_map = {uid: i for i, uid in enumerate(foul_uids)}        

        data['foul'].x = torch.tensor(foul_feats, dtype=torch.float)
        data['player_stint', 'committed_foul', 'foul'].edge_index = torch.tensor(edge_foul, dtype=torch.long).t().contiguous()
        data['player_stint', 'drew_foul', 'foul'].edge_index = torch.tensor(edge_foul_drawn, dtype=torch.long).t().contiguous() 


        shot_uids = []        
        shot_feats = []
        edge_shot = []      
        edge_assist = []    
        edge_block = []     

        query = """
            MATCH (g:Game {id: $game_id})
            MATCH (ps:PlayerStint)-[:TOOK_SHOT]->(s:Shot)
            WHERE s.id STARTS WITH toString($game_id) 
                AND NOT s:FreeThrow
            
            OPTIONAL MATCH (as:PlayerStint)-[:ASSISTED]->(s)
            OPTIONAL MATCH (bs:PlayerStint)-[:BLOCKED]->(s)
            OPTIONAL MATCH (s)-[:GENERATED_SCORE]->(sc:Score)

            RETURN 
                elementId(s) AS shot_id,
                elementId(ps) AS shooter_id,
                elementId(as) AS assist_id,
                elementId(bs) AS block_id,
                labels(s) AS labels,
                s.x AS x, 
                s.y AS y, 
                s.distance AS dist,
                s.local_clock AS local_clock,
                s.global_clock AS global_clock
            ORDER BY global_clock ASC
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for i, row in enumerate(results): 
            shot_uids.append(row['shot_id'])
                        
            is_made = 1.0 if 'Made' in row['labels'] else 0.0
            is_3pt = 1.0 if '3PT' in row['labels'] else 0.0
            is_2pt = 1.0 if '2PT' in row['labels'] else 0.0
            
            shot_feats.append([
                row['global_clock'], row['local_clock'],   
                row['x'], row['y'], row['dist'],    
                is_2pt, is_3pt, is_made
            ])

            if row['shooter_id'] in ps_map:
                edge_shot.append([ps_map[row['shooter_id']], i])
            
            if row['assist_id'] and row['assist_id'] in ps_map:
                edge_assist.append([ps_map[row['assist_id']], i])
                
            if row['block_id'] and row['block_id'] in ps_map:
                edge_block.append([ps_map[row['block_id']], i])

        shot_map = {uid: i for i, uid in enumerate(shot_uids)}        

        data['shot'].x = torch.tensor(shot_feats, dtype=torch.float)
        data['player_stint', 'took_shot', 'shot'].edge_index = torch.tensor(edge_shot, dtype=torch.long).t().contiguous()
        data['player_stint', 'assisted', 'shot'].edge_index = torch.tensor(edge_assist, dtype=torch.long).t().contiguous() 
        data['player_stint', 'blocked', 'shot'].edge_index = torch.tensor(edge_block, dtype=torch.long).t().contiguous() 


        ft_uids = []        
        ft_feats = []
        edge_ft = []    
        edge_foul_ft = []    

        query = """
            MATCH (g:Game {id: $game_id})
            MATCH (ps:PlayerStint)-[:TOOK_SHOT]->(ft:FreeThrow)
            WHERE ft.id STARTS WITH toString($game_id) 
            
            OPTIONAL MATCH (f:Foul)-[:CAUSED]->(ft)
            OPTIONAL MATCH (ft)-[:GENERATED_SCORE]->(sc:Score)

            RETURN 
                elementId(ft) AS ft_id,
                elementId(ps) AS shooter_id,
                labels(ft) AS labels,
                ft.local_clock AS local_clock,
                ft.global_clock AS global_clock,
                elementId(f) AS foul_id
            ORDER BY global_clock ASC
        """
        results = self.execute_read(query, {"game_id": self.game_id})
        for i, row in enumerate(results): 
            ft_uids.append(row['ft_id'])
            is_made = 1.0 if 'Made' in row['labels'] else 0.0            
            ft_feats.append([row['global_clock'], row['local_clock'], is_made])

            if row['shooter_id'] in ps_map:
                edge_ft.append([ps_map[row['shooter_id']], i])

            if row['foul_id'] and row['foul_id'] in foul_uids:
                edge_foul_ft.append([foul_map[row['foul_id']], i])

        ft_map = {uid: i for i, uid in enumerate(ft_uids)}        

        data['freethrow'].x = torch.tensor(ft_feats, dtype=torch.float)
        data['player_stint', 'took_shot', 'freethrow'].edge_index = torch.tensor(edge_ft, dtype=torch.long).t().contiguous()
        data['foul', 'caused', 'freethrow'].edge_index = torch.tensor(edge_foul_ft, dtype=torch.long).t().contiguous()


        return data