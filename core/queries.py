MERGE_TEAMS = """
    UNWIND $teams AS team 
    MERGE (t:Team {id: team.id})
    ON CREATE SET
        t.name = team.full_name,
        t.abbreviation = team.abbreviation

    MERGE (a:Arena {name: team.arena})
    MERGE (t)-[:PLAYS_IN]->(a)

    MERGE (c:City {name: team.city})
    MERGE (a)-[:LOCATED_IN]->(c)

    MERGE (s:State {name: team.state})
    MERGE (c)-[:LOCATED_IN]->(s)
"""


MERGE_GAME_SCHEDULE = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game

    MATCH (ht:Team {id: game.home_team_id})
    MATCH (at:Team {id: game.away_team_id})
    MATCH (a:Arena)<-[:PLAYS_IN]-(ht)

    MERGE (g:Game {id: game.game_id})
    ON CREATE SET
        g.date = datetime(game.datetime)

    MERGE (s)-[:HAS_GAME]-(g)
    MERGE (g)-[:AT]->(a)
    MERGE (ht)-[:PLAYS_HOME]->(g)
    MERGE (at)-[:PLAYS_AWAY]->(g)
"""


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


MERGE_NEXT_GAME_LINK = """   
    MATCH (s:Season {id: $season_id})-[:HAS_GAME]->(g:Game)<-[:PLAYS_HOME|PLAYS_AWAY]-(t:Team)
    WITH t, g ORDER BY g.date ASC     
    
    WITH t, collect(g) AS games, 
    UNWIND range(0, size(games) - 2) AS i        
    WITH t, 
        games[i] AS prev_g, 
        games[i+1] AS next_g,
        duration.between(games[i].date, games[i+1].date) AS delta_t

    MERGE (prev_g)-[:NEXT {time_since: delta_t}]->(next_g)
"""


MERGE_NEXT_PERIOD_LINK = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, p
    ORDER BY p.n ASC
    
    MATCH (p_next:Period {n: p.n + 1})
    WHERE (g)-[:HAS_PERIOD]->(p_next)
    MERGE (p)-[:NEXT]->(p_next)
"""

MERGE_NEXT_LINEUP_LINK = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    MATCH (l:LineUp)-[r:APPEARS_IN]->(p)
    MATCH (l)-[:PLAY_FOR]->(t:Team)

    WITH t, l, r.time AS time
    ORDER BY t.id, time ASC

    WITH t, collect(l) AS lineups
    UNWIND range(0, size(lineups) - 2) AS i
    WITH lineups[i] AS l_prev, lineups[i+1] AS l_next
    MERGE (l_prev)-[:NEXT]->(l_next)
"""


SET_GAME_DURATION = """
    MATCH (g:Game {id: $game_id})-[:HAS_PERIOD]->(p:Period)
    WITH g, min(p.start) AS first_start, max(p.end) AS last_end
    SET 
        g.start = first_start,
        g.end = last_end,
        g.duration = duration.between(first_start, last_end)
"""

