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

GET_TEAMS = """
    MATCH (g:Game {id: $game_id})
    MATCH (ht)-[:PLAYED_HOME]->(g)
    MATCH (at:Team)-[:PLAYED_AWAY]->(g)
    RETURN 
        ht.id AS home_team_id, 
        at.id AS away_team_id 
"""


MERGE_PERIODS = """
    MATCH (g:Game {id: $game_id})
    WITH g
    UNWIND $periods AS period

    WITH g, period, 
        toString($game_id) + "_" + toString(period.n) AS period_id,
        datetime(period.start) AS start,
        datetime(period.end) AS end

    MERGE (p:Period {id: period_id})
    ON CREATE SET
        p.n = period.n,
        p.start = start,
        p.duration = duration.between(start, end)

    FOREACH (_ IN CASE WHEN p.n = 1 THEN [1] ELSE [] END | SET p:RegularTime:Q1)
    FOREACH (_ IN CASE WHEN p.n = 2 THEN [1] ELSE [] END | SET p:RegularTime:Q2)
    FOREACH (_ IN CASE WHEN p.n = 3 THEN [1] ELSE [] END | SET p:RegularTime:Q3)
    FOREACH (_ IN CASE WHEN p.n = 4 THEN [1] ELSE [] END | SET p:RegularTime:Q4)
    FOREACH (_ IN CASE WHEN p.n > 4 THEN [1] ELSE [] END | SET p:OverTime)

    MERGE (p)-[:IN_GAME]->(g)


    WITH g,
        min(datetime(period.start)) AS first_start, 
        max(datetime(period.end)) AS last_end
    SET 
        g.start = first_start,
        g.duration = duration.between(first_start, last_end)


    WITH distinct g
    MATCH (p:Period)-[:IN_GAME]->(g)
    
    WITH p ORDER BY p.n ASC
    WITH collect(p) AS periods
    UNWIND range(0, size(periods) - 2) AS i

    WITH periods[i] AS current, periods[i+1] AS next
    MERGE (current)-[r:NEXT]->(next)
    ON CREATE SET 
        r.time_since = duration.between((current.start + current.duration), next.start)
"""


MERGE_STINTS = """
    MATCH (g:Game {id: $game_id})

    WITH g
    UNWIND $sides AS side
    CALL (g, side) {
        MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(g)
        WHERE t.id = side.team_id

        WITH g, t, side.lineups AS lineups
        CALL (g, t, lineups) {

            UNWIND range(0, size(lineups) - 1) AS i
            WITH i, g, t, 
                lineups[i] AS lineup,
                reduce(s = "", x IN lineups[i].ids | 
                    s + (CASE WHEN s="" THEN "" ELSE "_" END) + toString(x)) AS lineup_id

            MERGE (l:LineUp {id: lineup_id})
            MERGE (t)-[:HAS_LINEUP]->(l)
            FOREACH (pl_id IN lineup.ids |
                MERGE (pl:Player {id: pl_id})
                MERGE (pl)-[:MEMBER_OF]->(l)
            )

            WITH i, g, l, lineup
            MATCH (p:Period)-[:IN_GAME]->(g)
            WHERE p.n = lineup.period

            WITH i, g, p, l, lineup,
                l.id + "_" + toString(g.id) + "_" + toString(p.n) + "_" + toString(i) AS stint_id,
                CASE WHEN lineup.time = "" 
                    THEN p.start 
                    ELSE datetime(lineup.time)
                END AS time,
                duration(lineup.clock) AS clock

            MERGE (ls:LineUpStint {id: stint_id})
            ON CREATE SET
                ls._clock = clock,
                ls.clock = CASE WHEN p.n <= 4 
                    THEN 720.0 - clock.milliseconds / 1000.0
                    ELSE 300.0 - clock.milliseconds / 1000.0
                END,
                ls.global_clock = CASE WHEN p.n <= 4 
                    THEN p.n * 720.0 - clock.milliseconds / 1000.0
                    ELSE 2880.0 + (p.n - 4) * 300.0 - clock.milliseconds / 1000.0
                END,
                ls.time = time

            MERGE (l)-[:ON_COURT]->(ls)-[:IN_PERIOD]->(p)

            WITH p, ls
            ORDER BY ls.global_clock ASC
            WITH p, collect(ls) AS stints 
            UNWIND range(0, size(stints) - 1) AS j
            WITH p,  
                stints[j] AS current, 
                CASE WHEN j + 1 < size(stints) 
                    THEN stints[j + 1] 
                    ELSE NULL
                END AS next
            
            SET
                current.clock_duration = CASE 
                    WHEN next IS NOT NULL 
                    THEN next.clock - current.clock
                    ELSE CASE 
                        WHEN p.n <= 4
                        THEN 720.0 - current.clock 
                        ELSE 300.0 - current.clock
                    END
                END,
                current.time_duration = CASE WHEN next IS NOT NULL 
                    THEN duration.between(current.time, next.time)
                    ELSE duration.between(current.time, p.start + p.duration)
                END

            FOREACH (_ IN CASE WHEN next IS NOT NULL THEN [1] ELSE [] END |
                MERGE (current)-[:ON_COURT_NEXT]->(next)
            )
        
        }

    }


    WITH distinct g
    CALL (g) {
        MATCH (p:Period)-[:IN_GAME]->(g)    
        MATCH (ht:Team)-[:PLAYED_HOME]->(g)
        MATCH (at:Team)-[:PLAYED_AWAY]->(g)
        MATCH (ht)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(hs:LineUpStint)-[:IN_PERIOD]->(p)
        MATCH (at)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(as:LineUpStint)-[:IN_PERIOD]->(p)

        WITH hs, as,
            hs.global_clock + hs.clock_duration AS hs_end,
            as.global_clock + as.clock_duration AS as_end

        WHERE hs.global_clock < as_end AND as.global_clock < hs_end
        WITH hs, as, hs_end, as_end,
            hs.time + hs.time_duration AS hs_time_end,
            as.time + as.time_duration AS as_time_end
        
        WITH hs, as, hs_end, as_end, hs_time_end, as_time_end,
            CASE WHEN hs.global_clock > as.global_clock 
                THEN hs.global_clock ELSE as.global_clock 
            END AS max_start,
            CASE WHEN hs_end < as_end 
                THEN hs_end ELSE as_end 
            END AS min_end,
            CASE WHEN hs.time > as.time 
                THEN hs.time ELSE as.time 
            END AS delta_start,
            CASE WHEN hs_time_end < as_time_end 
                THEN hs_time_end ELSE as_time_end 
            END AS delta_end

        MERGE (hs)-[r:VS]-(as)
        SET 
            r.clock_duration = min_end - max_start,
            r.time_duration = duration.between(delta_start, delta_end)
    }


    WITH distinct g
    CALL (g) {
        MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(g)<-[:IN_GAME]-(p:Period)
        MATCH (t)-[:HAS_LINEUP]->(l:LineUp)-[:ON_COURT]->(ls:LineUpStint)-[:IN_PERIOD]->(p)
        MATCH (pl:Player)-[:MEMBER_OF]->(l)

        WITH g, p, t, pl, ls
        ORDER BY ls.global_clock ASC

        WITH g, p, t, pl, 
            collect(ls) AS l_stints
     
        UNWIND range(0, size(l_stints) -1) AS i
        WITH g, p, t, pl, l_stints, i, 
            CASE 
                WHEN i = 0 
                THEN 1 
                WHEN (l_stints[i].global_clock - (l_stints[i - 1].global_clock + l_stints[i - 1].clock_duration)) > 0.001 
                THEN 1 
                ELSE 0 
            END AS is_new_run

        WITH g, p, t, pl, l_stints, 
            collect(is_new_run) AS run_flags

        UNWIND range(0, size(l_stints) - 1) AS j
        WITH g, p, t, pl, l_stints[j] AS current,
            reduce(s = 0, x IN run_flags[0..j+1] | s + x) AS run_id

        WITH g, p, t, pl, run_id, 
            collect(current) AS sub_stints

        WITH g, p, t, pl, sub_stints,
            head(sub_stints) AS first_ls,
            last(sub_stints) AS last_ls

        WITH g, p, t, pl, sub_stints, first_ls, last_ls,
            toString(pl.id) + "_"  + toString(g.id) + "_" + toString(p.n) + "_" + toString(first_ls.global_clock) AS ps_id,
            reduce(d = 0.0, x IN sub_stints | d + x.clock_duration) AS total_clock_duration,
            duration.between(first_ls.time, last_ls.time + last_ls.time_duration) AS total_time_duration


        MERGE (ps:PlayerStint {id: ps_id})   
        MERGE (pl)-[:ON_COURT]->(ps)    
        ON CREATE SET 
            ps.global_clock = first_ls.global_clock,
            ps._clock = first_ls._clock,
            ps.clock = first_ls.clock,
            ps.time = first_ls.time,
            ps.clock_duration = total_clock_duration,
            ps.time_duration = total_time_duration

        FOREACH (sub IN sub_stints |
            MERGE (ps)-[:ON_COURT_WITH]->(sub)
        )
    }


    WITH distinct g
    CALL (g) {
        MATCH (p:Player)-[:ON_COURT]->(ps:PlayerStint)
        WHERE (ps)-[:ON_COURT_WITH]->(:LineUpStint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
        
        WITH DISTINCT p, ps
        ORDER BY ps.global_clock ASC
    
        WITH p, collect(ps) AS stints
        UNWIND range(0, size(stints) - 2) AS j
        WITH 
            stints[j] AS current, 
            stints[j + 1] AS next

        MERGE (current)-[r:NEXT]->(next)
        ON CREATE SET 
            r.clock_since = next.global_clock - (current.global_clock + current.clock_duration),
            r.time_since = duration.between(current.time + current.time_duration, next.time)
    }
"""