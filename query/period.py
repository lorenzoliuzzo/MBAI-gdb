import pandas as pd


MERGE_PERIOD_QUERY = """
    MATCH (g:Game {id: $game_id})
    WITH g
    UNWIND $periods AS period

    MERGE (p:Period {n: period.n})
    ON CREATE SET
        p.start = datetime(period.start),
        p.end = datetime(period.end),
        p.duration = duration.between(period.start, period.end)

    MERGE (g)-[:HAS_PERIOD]->(p)

    WITH g, p, period
    WHERE period.n <= 4
    SET p:RegularPeriod
    REMOVE p:OverTime

    WITH g, p, period
    WHERE period.n > 4
    SET p:OverTime
    REMOVE p:RegularPeriod

    WITH p
    RETURN count(p) AS period_count
"""

MERGE_NEXT_PERIOD_LINK_QUERY = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, p
    ORDER BY p.n ASC
    
    MATCH (p_next:Period {n: p.n + 1})
    WHERE (g)-[:HAS_PERIOD]->(p_next)
    MERGE (p)-[:NEXT]->(p_next)
    RETURN count(p) AS linked_count
"""


def get_periods(game_df):
    periods = []
    df = game_df[game_df["actionType"] == "period"]
    for n, game_df in df.groupby("period"):
        times = pd.to_datetime(game_df["timeActual"])
        p = {
            "n": n, 
            "start": times.iloc[0], 
            "end": times.iloc[1]
        }
        periods.append(p)
    return periods