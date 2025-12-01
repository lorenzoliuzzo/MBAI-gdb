SETUP = """
    CREATE CONSTRAINT team_id IF NOT EXISTS FOR (t:Team) REQUIRE t.id IS UNIQUE
    CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.id IS UNIQUE
    CREATE CONSTRAINT arena_name IF NOT EXISTS FOR (a:Arena) REQUIRE a.name IS UNIQUE
    CREATE CONSTRAINT city_name IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE
    CREATE CONSTRAINT state_name IF NOT EXISTS FOR (s:State) REQUIRE s.name IS UNIQUE
    CREATE CONSTRAINT season_id IF NOT EXISTS FOR (s:Season) REQUIRE s.id IS UNIQUE
    CREATE CONSTRAINT game_id IF NOT EXISTS FOR (g:Game) REQUIRE g.id IS UNIQUE
    CREATE INDEX IF NOT EXISTS FOR (g:Game) ON (g.date)
    CREATE CONSTRAINT lineup_id IF NOT EXISTS FOR (l:LineUp) REQUIRE l.id IS UNIQUE
"""
# CREATE INDEX IF NOT EXISTS FOR (ls:LineUpStint) ON (ls.global_seconds)


###############
# MERGE NODES #
###############

MERGE_TEAMS = """
    UNWIND $teams AS team 
    MERGE (t:Team {id: team.id})
    ON CREATE SET
        t.name = team.full_name,
        t.abbreviation = team.abbreviation

    MERGE (a:Arena {name: team.arena})
    MERGE (c:City {name: team.city})
    MERGE (s:State {name: team.state})
    MERGE (t)-[:HOME_ARENA]->(a)-[:IN]->(c)-[:IN]->(s)
"""


MERGE_SEASON = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game

    MERGE (g:Game {id: game.game_id})-[r:IN]->(s)
    SET g.date = datetime(game.datetime)

    WITH s, g, game
    MATCH (ht:Team {id: game.home_team_id})-[:HOME_ARENA]->(a:Arena)
    MATCH (at:Team {id: game.away_team_id})
    MERGE (g)-[:AT]->(a)
    MERGE (ht)-[:PLAYED_HOME]->(g)
    MERGE (at)-[:PLAYED_AWAY]->(g)

    WITH distinct s
    MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(g:Game)-[r:IN]->(s)
    WITH t, g ORDER BY g.date ASC

    WITH t, collect(g) AS games
    UNWIND range(0, size(games) - 2) AS i
    WITH t, games[i] AS prev, games[i+1] AS next

    MERGE (prev)-[r:NEXT {team_id: t.id}]->(next)
    SET r.since = duration.between(prev.date, next.date)
"""


MERGE_PERIODS = """
    MATCH (g:Game {id: $game_id})
    WITH g
    UNWIND $periods AS period

    MERGE (p:Period {id: $game_id + "_" + toString(period.n)})
    ON CREATE SET
        p.n = period.n,
        p.start = datetime(period.start),
        p.duration = duration.between(datetime(period.start), datetime(period.end))

    FOREACH (_ IN CASE WHEN p.n = 1 THEN [1] ELSE [] END | SET p:RegularTime:Q1)
    FOREACH (_ IN CASE WHEN p.n = 2 THEN [1] ELSE [] END | SET p:RegularTime:Q2)
    FOREACH (_ IN CASE WHEN p.n = 3 THEN [1] ELSE [] END | SET p:RegularTime:Q3)
    FOREACH (_ IN CASE WHEN p.n = 4 THEN [1] ELSE [] END | SET p:RegularTime:Q4)
    FOREACH (_ IN CASE WHEN p.n > 4 THEN [1] ELSE [] END | SET p:OverTime)

    MERGE (p)-[:IN]->(g)
    WITH g, min(datetime(period.start)) AS first_start, max(datetime(period.end)) AS last_end
    SET 
        g.start = first_start,
        g.duration = duration.between(first_start, last_end)

    WITH distinct g
    MATCH (p:Period)-[:IN]->(g)
    WITH p ORDER BY p.n ASC

    WITH collect(p) AS periods
    UNWIND range(0, size(periods) - 2) AS i
    WITH periods[i] AS prev, periods[i+1] AS next

    MERGE (prev)-[r:NEXT]->(next)
    ON CREATE SET 
        r.since = duration.between((prev.start + prev.duration), next.start)
"""



MERGE_LINEUPS = """
    MATCH (g:Game {id: $game_id})
    MATCH (ht:Team)-[:PLAYED_HOME]->(g)
    MATCH (at:Team)-[:PLAYED_AWAY]->(g)


    // ==========================================
    // PHASE 1: CREATE NODES (Sides Unwind)
    // ==========================================

    WITH g, [{team: ht, data: $home_lineups}, {team: at, data: $away_lineups}] AS sides
    UNWIND sides AS side
    WITH g, side.team AS t, side.data AS lineups
    CALL (g, t, lineups) {
        UNWIND range(0, size(lineups)-1) AS i
        WITH g, t, lineups, i, lineups[i] AS lineup

        WITH g, t, lineups, i, lineup,
            reduce(s = "", x IN lineup.ids | s + (CASE WHEN s="" THEN "" ELSE "_" END) + toString(x)) AS lineup_id
        
        MERGE (l:LineUp {id: lineup_id})
        MERGE (t)-[:TEAM_LINEUP]->(l)
        
        FOREACH (p_id IN lineup.ids |
            MERGE (pl:Player {id: p_id})
            MERGE (pl)-[:IN]->(l)
        )

        WITH g, t, lineups, i, lineup, l
        MATCH (p:Period {n: lineup.period})-[:IN]->(g)
        
        WITH g, t, lineups, i, lineup, l, p,
            l.id + "_" + toString(lineup.period) + "_" + toString(i) AS stint_id,
            CASE WHEN lineup.period <= 4 THEN (lineup.period - 1) * 720 ELSE 2880 + ((lineup.period - 5) * 300) END AS p_offset,
            CASE WHEN lineup.period <= 4 THEN 720 ELSE 300 END AS p_len

        MERGE (l)-[:HAD_STINT]->(ls:LineUpStint {id: stint_id})
        MERGE (ls)-[:IN]->(p)
        ON CREATE SET
            ls.time = CASE WHEN lineup.time = "" THEN p.start ELSE datetime(lineup.time) END,
            ls.clock = duration(lineup.clock),
            ls.global_clock = p_offset + (p_len - duration(lineup.clock).seconds)

        FOREACH (_ IN CASE 
            WHEN i = 0 THEN [1] 
            WHEN lineups[i].period <> lineups[i-1].period THEN [1] 
            ELSE [] 
        END | SET ls:Starter)
    }


    // ==========================================
    // PHASE 2: CHRONOLOGICAL LINKING (:NEXT)
    // ==========================================

    CALL (g) {
        MATCH (p:Period)-[:IN]->(g)
        MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(g)
        MATCH (t)-[:TEAM_LINEUP]->(:LineUp)-[:HAD_STINT]->(ls:LineUpStint)-[:IN]->(p)
        
        WITH t, p, ls ORDER BY ls.global_clock ASC
        WITH t, p, collect(ls) AS stints
        
        UNWIND range(0, size(stints)-2) AS j
        WITH stints[j] AS prev, stints[j+1] AS next
        
        MERGE (prev)-[:NEXT]->(next)
        SET prev.duration = prev.clock - next.clock
    }


    // ==========================================
    // PHASE 3: OPPONENT LINKING (:VS)
    // ==========================================
    CALL (g) {
        MATCH (p:Period)-[:IN]->(g)
        WITH g, p, 
            CASE 
                WHEN p.n <= 4 THEN p.n * 720 
                ELSE 2880 + ((p.n - 4) * 300) 
            END AS p_end_global

        MATCH (ht:Team)-[:PLAYED_HOME]->(g)
        MATCH (ht)-[:TEAM_LINEUP]->(:LineUp)-[:HAD_STINT]->(hs:LineUpStint)-[:IN]->(p)
        OPTIONAL MATCH (hs)-[:NEXT]->(hs_next)
        
        MATCH (at:Team)-[:PLAYED_AWAY]->(g)
        MATCH (at)-[:TEAM_LINEUP]->(:LineUp)-[:HAD_STINT]->(as:LineUpStint)-[:IN]->(p)
        OPTIONAL MATCH (as)-[:NEXT]->(as_next)

        WITH hs, as, p_end_global,
            hs.global_clock AS h_start, COALESCE(hs_next.global_clock, p_end_global) AS h_end,
            as.global_clock AS a_start, COALESCE(as_next.global_clock, p_end_global) AS a_end

        WHERE h_start < a_end AND a_start < h_end
        
        WITH hs, as, 
            CASE WHEN h_start > a_start THEN h_start ELSE a_start END AS overlap_start,
            CASE WHEN h_end < a_end THEN h_end ELSE a_end END AS overlap_end

        WITH hs, as, (overlap_end - overlap_start) AS seconds
        WHERE seconds > 0

        MERGE (hs)-[r:VS]-(as)
        SET r.duration = duration({seconds: seconds})
    }
"""