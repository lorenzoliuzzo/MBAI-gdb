import pandas as pd
from typing import List

from .common import get_teams


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


# MERGE_LINEUPS = """
#     MATCH (g:Game {id: $game_id})
#     MATCH (t:Team {id: $team_id})
#     WITH g, t
#     UNWIND $lineups AS lineup

#     MERGE (l:LineUp {ids: lineup.ids})
#     MERGE (l)-[:PLAY_FOR]->(t)
#     MERGE (l)-[:APPEARS_IN]->(g)
#     MERGE (l)-[:APPEARS_IN {time: datetime(lineup.time), clock: lineup.clock}]->(p:Period {n: $lineup.period})

#     WITH lineup, l
#     UNWIND lineup.player_ids AS p_id
#     MERGE (p:Player {id: p_id})
#     MERGE (p)-[:APPEARS_IN {time: lineup.time, clock: lineup.clock}]->(l)
# """


MERGE_LINEUPS_SIMPLE = """
    MATCH (g:Game {id: $game_id})
    MATCH (t:Team {id: $team_id})
    WITH g, t
    
    UNWIND $lineups AS lineup_data
    MERGE (l:LineUp {ids: lineup_data.ids})
    
    MERGE (l)-[:PLAY_FOR]->(t)
    MERGE (l)-[:APPEARS_IN]->(g)

    WITH l, lineup_data
    UNWIND lineup_data.ids AS p_id
    MERGE (p:Player {id: p_id})    
    MERGE (p)-[:IN_LINEUP]->(l)
"""


MERGE_LINEUPS = """
    MATCH (g:Game {id: $game_id})
    MATCH (t:Team {id: $team_id})
    WITH g, t
    
    UNWIND $lineups AS lineup
    MATCH (g)-[:HAS_PERIOD]->(p:Period {n: lineup.period})
    MERGE (l:LineUp {ids: lineup.ids})
    MERGE (l)-[:PLAY_FOR]->(t)
    MERGE (l)-[:APPEARS_IN]->(g)

    MERGE (l)-[r_lp:APPEARS_IN]->(p)
    SET
        r_lp.time = CASE
                        WHEN lineup.time = "" THEN g.start
                        ELSE datetime(lineup.time)
                    END,
        r_lp.clock = duration(lineup.clock)

    WITH lineup, l, g, p
    UNWIND lineup.ids AS p_id
    MERGE (pl:Player {id: p_id}) 
    MERGE (pl)-[r_pl:APPEARS_IN]->(l)
    SET
        r_pl.time = CASE
                        WHEN lineup.time = "" THEN g.start
                        ELSE datetime(lineup.time)
                    END,
        r_pl.clock = duration(lineup.clock)
"""


MERGE_NEXT_LINEUP_LINK = """
    MATCH (g:Game {id: $game_id})
    MATCH (g)<-[:APPEARS_IN]-(l:LineUp)-[:PLAY_FOR]->(t:Team)
    MATCH (l)-[r:APPEARS_IN]->(p:Period)
    WHERE (g)-[:HAS_PERIOD]->(p)

    WITH t, l, r.time AS time
    ORDER BY t.id, time ASC
    WITH t, collect(l) AS lineups
    UNWIND range(0, size(lineups) - 2) AS i
    WITH lineups[i] AS l_prev, lineups[i+1] AS l_next
    MERGE (l_prev)-[:NEXT]->(l_next)
"""


def create_lineups(session, game_id: int, starters: pd.DataFrame, subs: pd.DataFrame): 
    print(f"Creating `LineUp`'s for `Game` {game_id}...")
    
    ht_id, at_id = get_teams(session, game_id)
    for team_id in [ht_id, at_id]:
        starter_ids = starters.loc[starters['TEAM_ID'] == team_id, 'PLAYER_ID'].to_list()
        team_subs = subs[subs['teamId'] == team_id]
        
        lineups = extract_lineups(starter_ids, team_subs)
        MERGE_LINEUPS_TX = lambda tx:tx.run(MERGE_LINEUPS, 
            game_id=game_id, team_id=team_id, lineups=lineups
        )
        session.execute_write(MERGE_LINEUPS_TX)

    MERGE_NEXT_LINEUP_LINK_TX = lambda tx: tx.run(MERGE_NEXT_LINEUP_LINK, game_id=game_id)
    session.execute_write(MERGE_NEXT_LINEUP_LINK_TX)