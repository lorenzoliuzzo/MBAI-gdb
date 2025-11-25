import pandas as pd


def extract_periods(df):
    periods = []
    for n, period_df in df.groupby("period"):
        times = pd.to_datetime(period_df["timeActual"])
        p = {
            "n": int(n), 
            "start": times.iloc[0], 
            "end": times.iloc[1]
        }
        periods.append(p)
    return periods



MERGE_PERIOD = """
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

MERGE_NEXT_PERIOD_LINK = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, p
    ORDER BY p.n ASC
    
    MATCH (p_next:Period {n: p.n + 1})
    WHERE (g)-[:HAS_PERIOD]->(p_next)
    MERGE (p)-[:NEXT]->(p_next)
    RETURN count(p) AS linked_count
"""


def create_periods(session, game_id, periods_df): 
    print(f"Creating `Period`s for `Game` {game_id}...")

    periods = extract_periods(periods_df)
    if len(periods) < 4: 
        return 

    MERGE_PERIOD_TX = lambda tx: tx.run(MERGE_PERIOD, 
        game_id=game_id, periods=periods
    )
    session.execute_write(MERGE_PERIOD_TX)

    MERGE_NEXT_PERIOD_LINK_TX = lambda tx: tx.run(MERGE_NEXT_PERIOD_LINK, 
        game_id=game_id
    )
    session.execute_write(MERGE_NEXT_PERIOD_LINK_TX)