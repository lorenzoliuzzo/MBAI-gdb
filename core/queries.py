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
        t.city = team.city
        t.state = team.state
        
    MERGE (t)-[:HOME_ARENA]->(a:Arena {name: team.arena})
"""


MERGE_SEASON = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game

    MERGE (g:Game {id: game.game_id})-[:IN]->(s)
    SET g.date = datetime(game.datetime)

    WITH s, game, g
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
    WITH games[i] AS prev, games[i+1] AS next
    MERGE (prev)-[r:NEXT]->(next)
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


MERGE_LINEUP_STINTS = """
    MATCH (g:Game {id: $game_id})
    MATCH (ht:Team)-[:PLAYED_HOME]->(g)
    MATCH (at:Team)-[:PLAYED_AWAY]->(g)

    WITH g, [{team: ht, data: $home_lineups}, {team: at, data: $away_lineups}] AS sides
    UNWIND sides AS side
    WITH g, side.team AS t, side.data AS lineups
    CALL (g, t, lineups) {
        UNWIND range(0, size(lineups)-1) AS i
        WITH g, t, lineups, i, 
            reduce(s = "", 
                x IN lineups[i].ids | s + (CASE WHEN s="" THEN "" ELSE "_" END) + toString(x)
            ) AS lineup_id

        MERGE (t)-[:TEAM_LINEUP]->(l:LineUp {id: lineup_id})    
        FOREACH (p_id IN lineups[i].ids |
            MERGE (pl:Player {id: p_id})
            MERGE (pl)-[:IN]->(l)
        )

        MERGE (lp:LineUpPerformance {id: toString(g.id) + "_" + lineup_id})
        MERGE (l)-[:PERFORMED_IN]->(lp)-[:IN]->(g)

        WITH g, t, l, lp, lineups, i,
            l.id + "_" + toString(lineups[i].period) + "_" + toString(i) AS stint_id,
            CASE 
                WHEN lineups[i].period <= 4 THEN (lineups[i].period - 1) * 720 
                ELSE 2880 + ((lineups[i].period - 5) * 300) 
            END AS p_offset,
            CASE 
                WHEN lineups[i].period <= 4 THEN 720 
                ELSE 300 
            END AS p_len

        MATCH (p:Period {n: lineups[i].period})-[:IN]->(g)
        MERGE (lp)-[:COMPOSED_OF]->(ls:LineUpStint {id: stint_id})-[:IN]->(p)
        ON CREATE SET
            ls.time = 
                CASE 
                    WHEN lineups[i].time = "" THEN p.start 
                    ELSE datetime(lineups[i].time) 
                END,
            ls.clock = duration(lineups[i].clock),
            ls.global_clock = p_offset + (p_len - duration(lineups[i].clock).seconds)

        FOREACH (_ IN 
            CASE 
                WHEN i = 0 THEN [1] 
                WHEN lineups[i].period <> lineups[i-1].period THEN [1] 
                ELSE [] 
            END | 
            SET ls:Starter
        )

        WITH g, t, p, ls
        ORDER BY ls.global_clock ASC
        WITH g, t, p, collect(ls) AS stints
        UNWIND range(0, size(stints)-1) AS j
        WITH g, t, p, stints, j, 
            stints[j] AS current, 
            CASE 
                WHEN j < size(stints)-1 THEN stints[j+1] 
                ELSE NULL 
            END AS next

        FOREACH (_ IN CASE WHEN next IS NOT NULL THEN [1] ELSE [] END |
            MERGE (current)-[:NEXT]->(next)
        )

        WITH p, current, next, 
            CASE 
                WHEN next IS NOT NULL THEN (current.clock - next.clock)
                ELSE current.clock
            END AS clock_delta,
            CASE 
                WHEN next IS NOT NULL THEN duration.between(current.time, next.time)
                ELSE duration.between(current.time, (p.start + p.duration))
            END AS time_delta
             
        SET 
            current.clock_duration = clock_delta,
            current.time_duration = time_delta

        WITH current
        MATCH (lp:LineUpPerformance)-[:COMPOSED_OF]->(current)
        WITH lp, sum(current.clock_duration.seconds) AS total_sec
        SET lp.total_seconds = total_sec
    }

    WITH distinct g
    CALL (g) {
        MATCH (p:Period)-[:IN]->(g)    
        MATCH (ht:Team)-[:PLAYED_HOME]->(g)
        MATCH (at:Team)-[:PLAYED_AWAY]->(g)
        MATCH (ht)-[:TEAM_LINEUP]->(:LineUp)-[:PERFORMED_IN]->(:LineUpPerformance)-[:COMPOSED_OF]->(hs:LineUpStint)-[:IN]->(p)
        MATCH (at)-[:TEAM_LINEUP]->(:LineUp)-[:PERFORMED_IN]->(:LineUpPerformance)-[:COMPOSED_OF]->(as:LineUpStint)-[:IN]->(p)

        WITH hs, as,
            (hs.global_clock + hs.clock_duration.seconds) AS hs_end,
            (as.global_clock + as.clock_duration.seconds) AS as_end,
            (hs.time + hs.time_duration) AS hs_time_end,
            (as.time + as.time_duration) AS as_time_end

        WHERE hs.global_clock < as_end AND as.global_clock < hs_end

        WITH hs, as, hs_end, as_end, hs_time_end, as_time_end,
            CASE 
                WHEN hs.global_clock > as.global_clock THEN hs.global_clock 
                ELSE as.global_clock 
            END AS max_start,
            CASE 
                WHEN hs_end < as_end THEN hs_end 
                ELSE as_end 
            END AS min_end,
            CASE 
                WHEN hs.time > as.time THEN hs.time 
                ELSE as.time 
            END AS time_overlap_start,
            CASE 
                WHEN hs_time_end < as_time_end THEN hs_time_end 
                ELSE as_time_end 
            END AS time_overlap_end

        WITH hs, as, time_overlap_start, time_overlap_end, 
            (min_end - max_start) AS overlap_seconds
        WHERE overlap_seconds > 0

        MERGE (hs)-[r:VS]-(as)
        SET 
            r.clock_duration = duration({seconds: overlap_seconds}),
            r.time_duration = duration.between(time_overlap_start, time_overlap_end)
    }
"""


MERGE_PLAYER_STINTS = """
    MATCH (g:Game {id: $game_id})
    MATCH (ht:Team)-[:PLAYED_HOME]->(g)
    MATCH (at:Team)-[:PLAYED_AWAY]->(g)

    CALL (g, ht, at) {
        MATCH (t:Team)-[:TEAM_LINEUP]->(l:LineUp)-[:PERFORMED_IN]->(lp:LineUpPerformance)-[:COMPOSED_OF]->(ls:LineUpStint)-[:IN]->(p:Period)-[:IN]->(g)
        WHERE t = ht OR t = at
        MATCH (player:Player)-[:IN]->(l)
                
        WITH distinct g, t, player
        
        MERGE (pp:PlayerPerformance {id: toString(g.id) + "_" + toString(player.id)})
        MERGE (player)-[:PERFORMED_IN]->(pp)-[:IN_GAME]->(g)

        WITH g, t, player, pp
        MATCH (player)-[:IN]->(:LineUp)-[:PERFORMED_IN]->(:LineUpPerformance)-[:COMPOSED_OF]->(start_ls:LineUpStint)-[:IN]->(p:Period)-[:IN]->(g)
        
        OPTIONAL MATCH (prev_ls)-[:NEXT]->(start_ls)
        WHERE EXISTS {
            (player)-[:IN]->(:LineUp)-[:PERFORMED_IN]->(:LineUpPerformance)-[:COMPOSED_OF]->(prev_ls)
        }

        WITH g, t, player, pp, start_ls, p
        WHERE prev_ls IS NULL 
        
        MATCH path = (start_ls)-[:NEXT*0..]->(end_ls)
        WHERE ALL(node IN nodes(path) WHERE EXISTS {
            (player)-[:IN]->(:LineUp)-[:PERFORMED_IN]->(:LineUpPerformance)-[:COMPOSED_OF]->(node)
        }) AND NOT EXISTS {
            MATCH (end_ls)-[:NEXT]->(next_ls)
            WHERE EXISTS {
                (player)-[:IN]->(:LineUp)-[:PERFORMED_IN]->(:LineUpPerformance)-[:COMPOSED_OF]->(next_ls)
            }
        }

        WITH g, t, p, player, pp, start_ls, end_ls, nodes(path) as sub_stints,
            reduce(d = duration("PT0S"), s IN nodes(path) | d + s.clock_duration) AS total_clock_dur

        MERGE (pp)-[:COMPOSED_OF]->(ps:PlayerStint {id: toString(g.id) + "_" + toString(player.id) + "_" + toString(p.n) + "_" + toString(start_ls.global_clock)})
        MERGE (ps)-[:IN]->(p)
        ON CREATE SET
            ps.global_clock = start_ls.global_clock,
            ps.time = start_ls.time,
            ps.clock = start_ls.clock,
            ps.clock_duration = total_clock_dur,
            ps.time_duration = duration.between(start_ls.time, (end_ls.time + end_ls.time_duration))

        FOREACH (sub IN sub_stints |
            MERGE (ps)-[:WITH]->(sub)
        )

        WITH pp, sum(ps.clock_duration.seconds) AS total_sec
        SET pp.total_seconds = total_sec
    }

    WITH distinct g
    CALL (g) {
        MATCH (pp:PlayerPerformance)-[:IN_GAME]->(g)
        MATCH (pp)-[:COMPOSED_OF]->(ps:PlayerStint)
        
        WITH pp, ps
        ORDER BY ps.global_clock ASC
        
        WITH pp, collect(ps) AS stints
        UNWIND range(0, size(stints)-2) AS i
        WITH stints[i] AS current, stints[i+1] AS next
        
        MERGE (current)-[r:NEXT]->(next)
        
        WITH current, next, r,
            (next.global_clock - (current.global_clock + current.clock_duration.seconds)) AS gap_seconds
        
        SET 
            r.clock_since = duration({seconds: gap_seconds}),
            r.time_since = duration.between((current.time + current.time_duration), next.time)
    }

    WITH distinct g
    CALL (g) {
        MATCH (pp:PlayerPerformance)-[:IN_GAME]->(g)
        MATCH (pp)-[:COMPOSED_OF]->(ps:PlayerStint)
        
        MATCH (ps)-[:WITH]->(home_ls:LineUpStint)-[v:VS]-(opp_ls:LineUpStint)
        WITH ps, opp_ls, collect(v) AS vs_rels
        WITH ps, opp_ls,
            reduce(s = 0, x IN vs_rels | s + x.clock_duration.seconds) AS total_seconds,
            reduce(t = duration('PT0S'), x IN vs_rels | t + x.time_duration) AS total_time_duration
        WHERE total_seconds > 0

        MERGE (ps)-[r:VS]->(opp_ls)
        SET 
            r.clock_duration = duration({seconds: total_seconds}),
            r.time_duration = total_time_duration
    }
"""