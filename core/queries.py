SETUP_QUERIES = [
    "CREATE CONSTRAINT team_id IF NOT EXISTS FOR (t:Team) REQUIRE t.id IS UNIQUE",
    "CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.id IS UNIQUE",
    "CREATE CONSTRAINT arena_name IF NOT EXISTS FOR (a:Arena) REQUIRE a.name IS UNIQUE",
    "CREATE CONSTRAINT city_name IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT state_name IF NOT EXISTS FOR (s:State) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT season_id IF NOT EXISTS FOR (s:Season) REQUIRE s.id IS UNIQUE",
    "CREATE CONSTRAINT game_id IF NOT EXISTS FOR (g:Game) REQUIRE g.id IS UNIQUE",
    "CREATE INDEX IF NOT EXISTS FOR (g:Game) ON (g.date)",
    "CREATE CONSTRAINT lineup_id IF NOT EXISTS FOR (l:LineUp) REQUIRE l.id IS UNIQUE",
    "CREATE CONSTRAINT lineup_stint_id IF NOT EXISTS FOR (ls:LineUpStint) REQUIRE ls.id IS UNIQUE",
    "CREATE CONSTRAINT player_stint_id IF NOT EXISTS FOR (ps:PlayerStint) REQUIRE ps.id IS UNIQUE",
    "CREATE INDEX ls_timeline IF NOT EXISTS FOR (ls:LineUpStint) ON (ls.global_clock)",
    "CREATE INDEX ps_timeline IF NOT EXISTS FOR (ps:PlayerStint) ON (ps.global_clock)",
]


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
    SET r.time_since = duration.between(prev.date, next.date)
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
        r.time_since = duration.between((prev.start + prev.duration), next.start)
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
    // PHASE 2: CHRONOLOGICAL LINKING & DURATION
    // ==========================================

    CALL (g) {
        MATCH (p:Period)-[:IN]->(g)
        MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(g)
        MATCH (t)-[:TEAM_LINEUP]->(:LineUp)-[:HAD_STINT]->(ls:LineUpStint)-[:IN]->(p)
        
        WITH t, p, ls ORDER BY ls.global_clock ASC
        WITH t, p, collect(ls) AS stints
        
        UNWIND range(0, size(stints)-1) AS i
        WITH p, stints, i, stints[i] AS current
        
        WITH p, stints, i, current,
            CASE 
                WHEN i < size(stints)-1 THEN stints[i+1] 
                ELSE NULL 
            END AS next,
            CASE 
                WHEN i < size(stints)-1 THEN stints[i+1].clock 
                ELSE duration("PT0S") 
            END AS end_clock

        FOREACH (_ IN CASE WHEN next IS NOT NULL THEN [1] ELSE [] END |
            MERGE (current)-[:NEXT]->(next)
        )
        SET 
            current.clock_duration = current.clock - end_clock,
            current.time_duration = CASE 
                WHEN next IS NOT NULL THEN duration.between(current.time, next.time)
                ELSE duration.between(current.time, (p.start + p.duration))
            END
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

        WITH hs, hs_next, as, as_next, p, p_end_global,
            hs.global_clock AS h_start, COALESCE(hs_next.global_clock, p_end_global) AS h_end,
            as.global_clock AS a_start, COALESCE(as_next.global_clock, p_end_global) AS a_end

        WHERE h_start < a_end AND a_start < h_end
        
        WITH hs, hs_next, as, as_next, p,
            CASE WHEN h_start > a_start THEN h_start ELSE a_start END AS overlap_start,
            CASE WHEN h_end < a_end THEN h_end ELSE a_end END AS overlap_end
        
        WITH hs, hs_next, as, as_next, p, (overlap_end - overlap_start) AS seconds
        WHERE seconds > 0

        WITH hs, hs_next, as, as_next, p, seconds,
            CASE 
                WHEN hs_next IS NOT NULL THEN hs_next.time 
                ELSE (p.start + p.duration) 
            END AS hs_time_end,
            CASE 
                WHEN as_next IS NOT NULL THEN as_next.time 
                ELSE (p.start + p.duration) 
            END AS as_time_end

        WITH hs, as, seconds, hs_time_end, as_time_end,
            CASE WHEN hs.time > as.time THEN hs.time ELSE as.time END AS time_overlap_start,
            CASE WHEN hs_time_end < as_time_end THEN hs_time_end ELSE as_time_end END AS time_overlap_end

        MERGE (hs)-[r:VS]-(as)
        SET 
            r.clock_duration = duration({seconds: seconds}),
            r.time_duration = duration.between(time_overlap_start, time_overlap_end)
    }


    // ==========================================
    // PHASE 4: PLAYER STINTS
    // ==========================================

    CALL (g) {
        MATCH (p:Period)-[:IN]->(g)
        
        MATCH (t:Team)-[:TEAM_LINEUP]->(:LineUp)-[:HAD_STINT]->(start_ls:LineUpStint)-[:IN]->(p)
        MATCH (player:Player)-[:IN]->(:LineUp)-[:HAD_STINT]->(start_ls)

        WHERE NOT EXISTS {
            MATCH (prev_ls)-[:NEXT]->(start_ls)
            WHERE (player)-[:IN]->(:LineUp)-[:HAD_STINT]->(prev_ls)
        }

        MATCH path = (start_ls)-[:NEXT*0..]->(end_ls)
        WHERE ALL(
            ls IN nodes(path) WHERE EXISTS { 
                (player)-[:IN]->(:LineUp)-[:HAD_STINT]->(ls)
            }
        )

        AND NOT EXISTS {
            MATCH (end_ls)-[:NEXT]->(next_ls)
            WHERE (player)-[:IN]->(:LineUp)-[:HAD_STINT]->(next_ls)
        }

        WITH g, p, t, player, start_ls, nodes(path) AS sub_stints        
        WITH g, p, t, player, sub_stints,
            head(sub_stints) AS first,
            last(sub_stints) AS last,
            reduce(d = duration('PT0S'), s IN sub_stints | d + s.clock_duration) AS clock_duration

        OPTIONAL MATCH (last)-[:NEXT]->(bench_ls)
        MERGE (ps:PlayerStint {id: toString(g.id) + "_" + toString(player.id) + "_" + toString(p.n) + "_" + toString(first.global_clock)})
        ON CREATE SET
            ps.time = first.time,
            ps.clock = first.clock,
            ps.global_clock = first.global_clock,
            ps.clock_duration = clock_duration,
            ps.time_duration = CASE 
                WHEN bench_ls IS NOT NULL THEN duration.between(first.time, bench_ls.time)
                ELSE duration.between(first.time, (p.start + p.duration)) 
            END
        
        MERGE (player)-[:HAD_STINT]->(ps)
        MERGE (ps)-[:IN]->(p)
        // MERGE (ps)-[:FOR_TEAM]->(t)

        FOREACH (sub_stint IN sub_stints |
            MERGE (ps)-[:IN]->(sub_stint)
        )
    }


    // ==========================================
    // PHASE 5: LINK PLAYER STINTS (:NEXT)
    // ==========================================

    CALL (g) {
        MATCH (player:Player)-[:HAD_STINT]->(ps:PlayerStint)
        WHERE (ps)-[:IN]->(:Period)-[:IN]->(g)
        
        WITH player, ps
        ORDER BY ps.global_clock ASC
        
        WITH player, collect(ps) AS stints
        WHERE size(stints) > 1
        
        UNWIND range(0, size(stints)-2) AS i
        WITH stints[i] AS current, stints[i+1] AS next
        
        WITH current, next, 
            (next.global_clock - (current.global_clock + current.clock_duration.seconds)) AS clock_seconds_gap

        WITH current, next, clock_seconds_gap,
            CASE WHEN current.time_duration IS NOT NULL 
                THEN duration.between((current.time + current.time_duration), next.time)
                ELSE NULL 
            END AS time_duration_gap
                          
        MERGE (current)-[r:NEXT]->(next)
        SET 
            r.clock_since = duration({seconds: clock_seconds_gap}),
            r.time_since = time_duration_gap
    }


    // ==========================================
    // PHASE 6: PLAYER STINT VS OPPONENT LINEUP
    // ==========================================
    CALL (g) {
        MATCH (p:Period)-[:IN]->(g)
        MATCH (ps:PlayerStint)-[:IN]->(p)
        
        MATCH (ps)-[:IN]->(home_ls:LineUpStint)-[v:VS]-(opp_ls:LineUpStint)
        
        WITH ps, opp_ls, collect(v) AS vs_rels
        
        WITH ps, opp_ls,
            reduce(s = 0, x IN vs_rels | s + x.clock_duration.seconds) AS total_seconds,
            reduce(t = duration('PT0S'), x IN vs_rels | t + x.time_duration) AS total_time_duration

        MERGE (ps)-[r:VS]->(opp_ls)
        SET 
            r.clock_duration = duration({seconds: total_seconds}),
            r.time_duration = total_time_duration
    }
"""