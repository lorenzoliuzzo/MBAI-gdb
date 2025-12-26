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
                toString(g.id) + "_" + toString(p.n) + "_" + l.id + "_" + toString(i) AS stint_id
            
            MERGE (ls:LineUpStint {id: stint_id})
            ON CREATE SET
                ls.clock = duration(lineup.clock),
                ls.local_clock = lineup.local_clock,
                ls.global_clock = lineup.global_clock,
                ls.start_time = CASE WHEN lineup.time = "" 
                    THEN p.start 
                    ELSE datetime(lineup.time)
                END

            MERGE (l)-[:ON_COURT]->(ls)
            MERGE (ls)-[:IN_PERIOD]->(p)

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
                current.clock_duration = CASE WHEN next IS NOT NULL 
                    THEN next.local_clock - current.local_clock
                    ELSE CASE WHEN p.n <= 4
                        THEN 720.0 - current.local_clock 
                        ELSE 300.0 - current.local_clock
                    END
                END,
                current.time_duration = CASE WHEN next IS NOT NULL 
                    THEN duration.between(current.start_time, next.start_time)
                    ELSE duration.between(current.start_time, p.start + p.duration)
                END,
                current.end_time = CASE WHEN next IS NOT NULL
                    THEN next.start_time
                    ELSE (p.start + p.duration)
                END
                
            FOREACH (_ IN CASE WHEN next IS NOT NULL THEN [1] ELSE [] END |
                MERGE (current)-[:ON_COURT_NEXT]->(next)
            )
        }
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
                WHEN i = 0 THEN 1 
                WHEN l_stints[i].global_clock > l_stints[i-1].global_clock + l_stints[i-1].clock_duration THEN 1
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
            toString(g.id) + "_" + toString(p.n) + "_" + toString(pl.id) + "_" + toString(first_ls.global_clock) AS ps_id,
            reduce(d = 0.0, x IN sub_stints | d + x.clock_duration) AS total_clock_duration,
            duration.between(first_ls.start_time, last_ls.end_time) AS total_time_duration

        MERGE (ps:PlayerStint {id: ps_id})   
        MERGE (pl)-[:ON_COURT]->(ps)    
        ON CREATE SET 
            ps.clock = first_ls.clock,
            ps.local_clock = first_ls.local_clock,
            ps.global_clock = first_ls.global_clock,
            ps.start_time = first_ls.start_time,
            ps.clock_duration = total_clock_duration,
            ps.time_duration = total_time_duration,
            ps.end_time = first_ls.start_time + total_time_duration

        FOREACH (sub IN sub_stints | MERGE (ps)-[:ON_COURT_WITH]->(sub))
    }

    WITH distinct g
    CALL (g) {
        MATCH (entity:LineUp)-[:ON_COURT]->(stint:LineUpStint)
        WHERE (stint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
        RETURN entity, stint

        UNION

        MATCH (entity:Player)-[:ON_COURT]->(stint:PlayerStint)
        WHERE (stint)-[:ON_COURT_WITH]->(:LineUpStint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
        RETURN entity, stint
    }

    WITH entity, stint
    ORDER BY stint.global_clock ASC

    WITH entity, collect(stint) AS stints
    UNWIND range(0, size(stints) - 2) AS j
    WITH 
        stints[j] AS current, 
        stints[j + 1] AS next

    MERGE (current)-[r:NEXT]->(next)
    ON CREATE SET 
        r.clock_since = next.global_clock - (current.global_clock + current.clock_duration),
        r.time_since = duration.between(current.end_time, next.start_time)
"""


MERGE_JUMPBALLS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $jumpballs AS jb

    WITH g, jb,
        toString(g.id) + "_" + toString(jb.period) + "_" + jb.clock + "_jb_" + 
        COALESCE(toString(jb.won_id), "0") AS jb_id

    MERGE (j:Action:JumpBall {id: jb_id})
    ON CREATE SET 
        j.time = datetime(jb.time),
        j.clock = duration(jb.clock),
        j.local_clock = jb.local_clock,
        j.global_clock = jb.global_clock

    FOREACH (_ IN CASE WHEN jb.subtype = 'recovered' THEN [1] ELSE [] END | SET j:Recovered)

    FOREACH (_ IN CASE WHEN jb.descriptor = 'startperiod' THEN [1] ELSE [] END | SET j:StartPeriod)
    FOREACH (_ IN CASE WHEN jb.descriptor = 'heldball' THEN [1] ELSE [] END | SET j:HeldBall) 
    FOREACH (_ IN CASE WHEN jb.descriptor = 'unclearpass' THEN [1] ELSE [] END | SET j:UnclearPass)

    WITH g, j, jb
    CALL (g, j, jb) {
        WITH g, j, jb
        WHERE jb.won_id IS NOT NULL AND jb.won_id <> 0
        
        MATCH (p:Period {n: jb.period})-[:IN_GAME]->(g)
        MATCH (pl:Player {id: jb.won_id})
        
        MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls:LineUpStint)
        WHERE 
            (ls)-[:IN_PERIOD]->(p) AND 
            ls.start_time <= jb.time AND jb.time < ls.end_time
        
        MERGE (ps)-[:WON_JUMPBALL]->(j)
    }

    WITH g, j, jb
    CALL (g, j, jb) {
        WITH g, j, jb
        WHERE jb.lost_id IS NOT NULL AND jb.lost_id <> 0
        
        MATCH (p:Period {n: jb.period})-[:IN_GAME]->(g)
        MATCH (pl:Player {id: jb.lost_id})
        
        MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls:LineUpStint)
        WHERE 
            (ls)-[:IN_PERIOD]->(p) AND 
            ls.start_time <= jb.time AND jb.time < ls.end_time
        
        MERGE (ps)-[:LOST_JUMPBALL]->(j)
    }

    WITH g, j, jb
    CALL (g, j, jb) {
        WITH g, j, jb
        WHERE jb.team_id IS NOT NULL AND jb.team_id <> 0
        
        MATCH (p:Period {n: jb.period})-[:IN_GAME]->(g)
        MATCH (team:Team {id: jb.team_id})
        
        MATCH (team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
        WHERE 
            (ls)-[:IN_PERIOD]->(p) AND 
            ls.start_time <= jb.time AND jb.time < ls.end_time

        CALL (jb, ls, j) {
            WITH jb, ls, j
            WHERE jb.recovered_id IS NOT NULL AND jb.recovered_id <> 0
            MATCH (pl:Player {id: jb.recovered_id})
            MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
            MERGE (ps)-[:RECOVERED_JUMPBALL]->(j)
        }

        CALL (jb, ls, j) {
            WITH jb, ls, j
            WHERE jb.recovered_id IS NULL OR jb.recovered_id = 0
            MERGE (ls)-[:RECOVERED_JUMPBALL]->(j)
        }
    }
"""


MERGE_VIOLATIONS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $violations AS vio

    WITH g, vio,
        toString(g.id) + "_" + toString(vio.period) + "_" + vio.clock + "_violation_" + 
        COALESCE(toString(NULLIF(vio.player_id, 0)), toString(vio.team_id)) AS vio_id

    MERGE (v:Action:Violation {id: vio_id})
    ON CREATE SET 
        v.time = datetime(vio.time),
        v.clock = duration(vio.clock),
        v.local_clock = vio.local_clock,
        v.global_clock = vio.global_clock

    FOREACH (_ IN CASE WHEN vio.subtype = 'kicked ball' THEN [1] ELSE [] END | SET v:KickedBall)
    FOREACH (_ IN CASE WHEN vio.subtype = 'delay-of-game' THEN [1] ELSE [] END | SET v:DelayOfGame)
    FOREACH (_ IN CASE WHEN vio.subtype = 'lane' THEN [1] ELSE [] END | SET v:LaneViolation)
    FOREACH (_ IN CASE WHEN vio.subtype = 'goaltending' THEN [1] ELSE [] END | SET v:Goaltending)
    FOREACH (_ IN CASE WHEN vio.subtype = 'defensive goaltending' THEN [1] ELSE [] END | SET v:DefensiveGoaltending)
    FOREACH (_ IN CASE WHEN vio.subtype = 'double dribble' THEN [1] ELSE [] END | SET v:DoubleDribble)
    FOREACH (_ IN CASE WHEN vio.subtype = 'jump ball' THEN [1] ELSE [] END | SET v:JumpBallViolation)

    WITH g, v, vio
    MATCH (p:Period {n: vio.period})-[:IN_GAME]->(g)
    MATCH (team:Team {id: vio.team_id})
    
    MATCH (team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= vio.time AND vio.time < ls.end_time

    CALL (vio, v, ls) {
        WITH vio, v, ls
        WHERE vio.player_id IS NOT NULL AND vio.player_id <> 0
        
        MATCH (pl:Player {id: vio.player_id})
        MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
        
        MERGE (ps)-[:COMMITTED_VIOLATION]->(v)
    }

    CALL (vio, v, ls) {
        WITH vio, v, ls
        WHERE vio.player_id = 0 OR vio.player_id IS NULL
        MERGE (ls)-[:COMMITTED_VIOLATION]->(v)
    }
"""


MERGE_FOULS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $fouls AS foul

    WITH g, foul,
        toString(g.id) + "_" + toString(foul.period) + "_" + foul.clock + "_foul_" + 
        COALESCE(toString(NULLIF(foul.player_id, 0)), toString(foul.team_id)) AS foul_id

    MERGE (f:Action:Foul {id: foul_id})
    ON CREATE SET 
        f.time = datetime(foul.time),
        f.clock = duration(foul.clock),
        f.local_clock = foul.local_clock,
        f.global_clock = foul.global_clock

    FOREACH (_ IN CASE WHEN foul.subtype = 'offensive' THEN [1] ELSE [] END | SET f:Offensive) 
    // maybe do a causal link?
    
    FOREACH (_ IN CASE WHEN foul.subtype = 'technical' THEN [1] ELSE [] END | SET f:Technical)
    FOREACH (_ IN CASE WHEN foul.subtype = 'personal' THEN [1] ELSE [] END | SET f:Personal)
    FOREACH (_ IN CASE WHEN foul.subtype = 'flagrant' THEN [1] ELSE [] END | SET f:Flagrant)

    FOREACH (_ IN CASE WHEN foul.descriptor = 'shooting' THEN [1] ELSE [] END | SET f:Shooting)
    FOREACH (_ IN CASE WHEN foul.descriptor = 'loose ball' THEN [1] ELSE [] END | SET f:LooseBall)
    FOREACH (_ IN CASE WHEN foul.descriptor = 'take' THEN [1] ELSE [] END | SET f:Take)
    FOREACH (_ IN CASE WHEN foul.descriptor = 'defensive-3-second' THEN [1] ELSE [] END | SET f:Def3Sec)
    FOREACH (_ IN CASE WHEN foul.descriptor = 'charge' THEN [1] ELSE [] END | SET f:Charge)

    WITH g, f, foul
    MATCH (p:Period {n: foul.period})-[:IN_GAME]->(g)
    MATCH (t:Team {id: foul.team_id})
    
    MATCH (t)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= foul.time AND foul.time < ls.end_time

    CALL (foul, f, ls) {
        WITH foul, f, ls
        MATCH (pl:Player {id: foul.player_id})
        MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
        MERGE (ps)-[:COMMITTED_FOUL]->(f)
    }

    CALL (foul, f, ls) {
        WITH foul, f, ls
        WHERE 
            foul.player_id = 0 OR
            foul.player_id IS NULL OR
            foul.player_id = foul.team_id
        
        MERGE (ls)-[:COMMITTED_FOUL]->(f)
    }

    WITH g, f, foul, p
    CALL (g, f, foul, p) {
        WITH g, f, foul, p
        WHERE foul.drawn_id IS NOT NULL AND foul.drawn_id <> 0
        
        MATCH (victim:Player {id: foul.drawn_id})
        MATCH (victim)-[:ON_COURT]->(v_ps:PlayerStint)-[:ON_COURT_WITH]->(v_ls:LineUpStint)
        WHERE 
            (v_ls)-[:IN_PERIOD]->(p) AND
            v_ls.start_time <= foul.time AND foul.time < v_ls.end_time

        MERGE (v_ps)-[:DREW_FOUL]->(f)
    }
"""


MERGE_SHOTS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $shots as shot
    
    WITH g, shot, 
        toString(g.id) + "_" + toString(shot.period) + "_" + shot.clock + "_shot_" + toString(shot.player_id) AS shot_id

    MERGE (s:Action:Shot {id: shot_id})
    ON CREATE SET 
        s.time = datetime(shot.time),
        s.clock = duration(shot.clock),
        s.local_clock = shot.local_clock,
        s.global_clock = shot.global_clock,
        s.x = shot.x, 
        s.y = shot.y,
        s.distance = shot.distance

    WITH g, s, shot
    MATCH (p:Period {n: shot.period})-[:IN_GAME]->(g)
    MATCH (t:Team {id: shot.team_id})
    MATCH (t)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= s.time AND s.time < ls.end_time
        
    WITH g, s, shot, ls, p, t
    MATCH (:Player {id: shot.player_id})-[:ON_COURT]->(ps:PlayerStint)
    WHERE (ps)-[:ON_COURT_WITH]->(ls)    
    MERGE (ps)-[:TOOK_SHOT]->(s)

    WITH shot, ls, s, p, t

    CALL (shot, ls, s) {
        WITH shot, ls, s 
        WHERE shot.assist_id IS NOT NULL
        MATCH (assister:Player {id: shot.assist_id})-[:ON_COURT]->(as:PlayerStint)-[:ON_COURT_WITH]->(ls)
        MERGE (as)-[:ASSISTED]->(s)
    }

    CALL (shot, p, s, t) {
        WITH shot, p, s, t 
        WHERE shot.block_id IS NOT NULL
        
        MATCH (ot:Team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ols:LineUpStint)
        WHERE 
            ot <> t AND 
            (ols)-[:IN_PERIOD]->(p) AND
            ols.start_time <= s.time AND s.time < ols.end_time
        
        MATCH (blocker:Player {id: shot.block_id})
        MATCH (blocker)-[:ON_COURT]->(bs:PlayerStint)-[:ON_COURT_WITH]->(ols)
        MERGE (bs)-[:BLOCKED]->(s)
    }

    FOREACH (_ IN CASE WHEN shot.type = '2pt' THEN [1] ELSE [] END | SET s:`2PT`)
    FOREACH (_ IN CASE WHEN shot.type = '3pt' THEN [1] ELSE [] END | SET s:`3PT`)
    FOREACH (_ IN CASE WHEN shot.result = 'Made' THEN [1] ELSE [] END | SET s:Made)
    FOREACH (_ IN CASE WHEN shot.result = 'Missed' THEN [1] ELSE [] END | SET s:Missed)

    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'driving' THEN [1] ELSE [] END | SET s:Driving)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'running' THEN [1] ELSE [] END | SET s:Running)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'cutting' THEN [1] ELSE [] END | SET s:Cutting)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'step back' THEN [1] ELSE [] END | SET s:StepBack)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'pullup' THEN [1] ELSE [] END | SET s:PullUp)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'turnaround' THEN [1] ELSE [] END | SET s:TurnAround)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'reverse' THEN [1] ELSE [] END | SET s:Reverse)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'fadeaway' THEN [1] ELSE [] END | SET s:Fadeaway)

    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'bank' THEN [1] ELSE [] END | SET s:Bank)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'floating' THEN [1] ELSE [] END | SET s:Floater)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'finger roll' THEN [1] ELSE [] END | SET s:FingerRoll)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'alley-oop' THEN [1] ELSE [] END | SET s:AlleyOop)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'tip' THEN [1] ELSE [] END | SET s:Tip)
    FOREACH (_ IN CASE WHEN shot.descriptor CONTAINS 'putback' THEN [1] ELSE [] END | SET s:PutBack)
"""


MERGE_FREETHROWS = """
    MATCH (g:Game {id: $game_id})
    UNWIND range(0, size($shots)-1) as i
    WITH g, $shots[i] as ft, i
    WITH g, ft, i,
        toString(g.id) + "_" + toString(ft.period) + "_" + ft.clock + "_ft_" + toString(ft.player_id) + "_" + toString(i) AS ft_id

    MERGE (s:Action:Shot:FreeThrow {id: ft_id})
    ON CREATE SET 
        s.time = datetime(ft.time),
        s.clock = duration(ft.clock),
        s.local_clock = ft.local_clock,
        s.global_clock = ft.global_clock

    FOREACH (_ IN CASE WHEN ft.result = 'Made' THEN [1] ELSE [] END | SET s:Made)
    FOREACH (_ IN CASE WHEN ft.result = 'Missed' THEN [1] ELSE [] END | SET s:Missed)

    WITH g, s, ft
    MATCH (p:Period {n: ft.period})-[:IN_GAME]->(g)
    MATCH (t:Team {id: ft.team_id})
    MATCH (t)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= s.time AND s.time < ls.end_time
        
    WITH g, s, ft, ls
    MATCH (shooter:Player {id: ft.player_id})
    MATCH (shooter)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)    
    MERGE (ps)-[:TOOK_SHOT]->(s)

    WITH g, s
    MATCH (f:Action:Foul)
    WHERE 
        f.id STARTS WITH toString(g.id) AND
        f.global_clock = s.global_clock
    
    MERGE (f)-[:CAUSED]->(s)
"""


MERGE_REBOUNDS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $rebounds AS reb

    WITH g, reb,
        toString(g.id) + "_" + toString(reb.period) + "_" + reb.clock + "_reb_" + 
        COALESCE(toString(NULLIF(reb.player_id, 0)), toString(reb.team_id)) AS reb_id

    MERGE (r:Action:Rebound {id: reb_id})
    ON CREATE SET 
        r.time = datetime(reb.time),
        r.clock = duration(reb.clock),
        r.local_clock = reb.local_clock,
        r.global_clock = reb.global_clock

    FOREACH (_ IN CASE WHEN reb.subtype = 'offensive' THEN [1] ELSE [] END | SET r:Offensive)
    FOREACH (_ IN CASE WHEN reb.subtype = 'defensive' THEN [1] ELSE [] END | SET r:Defensive)

    WITH g, r, reb
    MATCH (p:Period {n: reb.period})-[:IN_GAME]->(g)
    MATCH (t:Team {id: reb.team_id})
    
    MATCH (t)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= r.time AND r.time < ls.end_time 
        
    CALL (reb, r, ls) {
        WITH reb, r, ls
        MATCH (pl:Player {id: reb.player_id}) 
        MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
        MERGE (ps)-[:REBOUNDED]->(r)
    }

    CALL (reb, r, ls) {
        WITH reb, r, ls
        WHERE reb.player_id = 0 OR reb.player_id IS NULL 
        MERGE (ls)-[:REBOUNDED]->(r)
    }

    WITH g, r
    CALL (g, r) {
        WITH g, r
        
        MATCH (s:Shot:Missed)
        WHERE s.id STARTS WITH toString(g.id) 
            AND s.global_clock <= r.global_clock
            AND s.global_clock >= r.global_clock - 10.0
            AND NOT EXISTS {
                MATCH (other:Rebound)-[:REBOUND_OF]->(s)
                WHERE other.id <> r.id
            }
        
        WITH s ORDER BY s.global_clock DESC LIMIT 1
        MERGE (r)-[:REBOUND_OF]->(s)
    }
"""


MERGE_TURNOVERS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $turnovers AS tov

    WITH g, tov,
        toString(g.id) + "_" + toString(tov.period) + "_" + tov.clock + "_" + "tov_" + 
        COALESCE(toString(NULLIF(tov.player_id, 0)), toString(tov.team_id)) AS tov_id

    MERGE (t:Action:TurnOver {id: tov_id})
    ON CREATE SET 
        t.time = datetime(tov.time),
        t.clock = duration(tov.clock),
        t.local_clock = tov.local_clock,
        t.global_clock = tov.global_clock

    FOREACH (_ IN CASE WHEN tov.subtype = 'bad pass' THEN [1] ELSE [] END | SET t:BadPass)
    FOREACH (_ IN CASE WHEN tov.subtype = 'lost ball' THEN [1] ELSE [] END | SET t:LostBall)
    FOREACH (_ IN CASE WHEN tov.subtype = 'traveling' THEN [1] ELSE [] END | SET t:Traveling)
    FOREACH (_ IN CASE WHEN tov.subtype = 'out-of-bounds' THEN [1] ELSE [] END | SET t:OutOfBounds)
    FOREACH (_ IN CASE WHEN tov.subtype = 'offensive foul' THEN [1] ELSE [] END | SET t:OffensiveFoul)
    FOREACH (_ IN CASE WHEN tov.subtype = 'shot clock' THEN [1] ELSE [] END | SET t:ShotClock)

    FOREACH (_ IN CASE WHEN tov.descriptor = 'lost ball' THEN [1] ELSE [] END | SET t:LostBall)
    FOREACH (_ IN CASE WHEN tov.descriptor = 'bas pass' THEN [1] ELSE [] END | SET t:BadPass)
    FOREACH (_ IN CASE WHEN tov.descriptor = 'step' THEN [1] ELSE [] END | SET t:Step)

    WITH g, t, tov
    MATCH (p:Period {n: tov.period})-[:IN_GAME]->(g)
    MATCH (team:Team {id: tov.team_id})
    
    MATCH (team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= tov.time AND tov.time < ls.end_time
        
    OPTIONAL MATCH (pl:Player {id: tov.player_id})
    OPTIONAL MATCH (pl)-[:ON_COURT]->(ps:PlayerStint)-[:ON_COURT_WITH]->(ls)
    
    FOREACH (_ IN CASE WHEN ps IS NOT NULL THEN [1] ELSE [] END | MERGE (ps)-[:LOST_BALL]->(t))
    FOREACH (_ IN CASE WHEN ps IS NULL THEN [1] ELSE [] END | MERGE (ls)-[:LOST_BALL]->(t))

    WITH g, t, tov, p
    CALL (g, t, tov, p) {
        WITH g, t, tov, p
        WHERE tov.steal_id IS NOT NULL AND tov.steal_id <> 0
        
        MATCH (stealer:Player {id: tov.steal_id})
        MATCH (stealer)-[:ON_COURT]->(s_ps:PlayerStint)
        
        MATCH (s_ps)-[:ON_COURT_WITH]->(s_ls:LineUpStint)
        WHERE 
            (s_ls)-[:IN_PERIOD]->(p) AND
            s_ls.start_time <= tov.time AND tov.time < s_ls.end_time

        MERGE (s_ps)-[:STOLE_BALL]->(t)
    }
"""


MERGE_TIMEOUTS = """
    MATCH (g:Game {id: $game_id})
    UNWIND $timeouts AS to

    WITH g, to,
        toString(g.id) + "_" + toString(to.period) + "_" + to.clock + "_timeout_" + toString(to.team_id) AS to_id

    MERGE (t:Action:TimeOut {id: to_id})
    ON CREATE SET 
        t.time = datetime(to.time),
        t.clock = duration(to.clock),
        t.local_clock = to.local_clock,
        t.global_clock = to.global_clock

    FOREACH (_ IN CASE WHEN to.subtype = 'full' THEN [1] ELSE [] END | SET t:FullTimeOut)
    FOREACH (_ IN CASE WHEN to.subtype = 'short' THEN [1] ELSE [] END | SET t:ShortTimeOut)

    WITH g, t, to
    MATCH (p:Period {n: to.period})-[:IN_GAME]->(g)
    MATCH (team:Team {id: to.team_id})
    
    MATCH (team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(ls:LineUpStint)
    WHERE 
        (ls)-[:IN_PERIOD]->(p) AND
        ls.start_time <= to.time AND to.time < ls.end_time

    MERGE (ls)-[:CALLED_TIMEOUT]->(t)
"""


MERGE_SCORES = """
    MATCH (g:Game {id: $game_id})
    MATCH (home:Team)-[:PLAYED_HOME]->(g)
    MATCH (away:Team)-[:PLAYED_AWAY]->(g)
    
    MATCH (scoring_team:Team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(scoring_ls:LineUpStint)<-[:ON_COURT_WITH]-(:PlayerStint)-[:TOOK_SHOT]->(s:Shot:Made)
    MATCH (scoring_ls)-[:IN_PERIOD]->(p:Period)-[:IN_GAME]->(g)
    WHERE scoring_ls.start_time <= s.time AND s.time < scoring_ls.end_time

    WITH g, home, away, s, p, scoring_team, scoring_ls
    ORDER BY scoring_ls.global_clock DESC
    WITH g, home, away, s, p, scoring_team, head(collect(scoring_ls)) AS scoring_ls

    MATCH (opp_team:Team)-[:HAS_LINEUP]->(:LineUp)-[:ON_COURT]->(opp_ls:LineUpStint)
    WHERE 
        opp_team.id <> scoring_team.id AND
        (opp_ls)-[:IN_PERIOD]->(p) AND
        opp_ls.start_time <= s.time AND s.time < opp_ls.end_time

    WITH g, home, away, s, p, scoring_team, scoring_ls, opp_ls
    ORDER BY opp_ls.global_clock DESC
    WITH g, home, away, s, p, scoring_team, scoring_ls, head(collect(opp_ls)) AS opp_ls

    WITH g, home, away, s, p, scoring_team, 
        CASE WHEN scoring_team.id = home.id THEN scoring_ls ELSE opp_ls END AS home_stint,
        CASE WHEN scoring_team.id = away.id THEN scoring_ls ELSE opp_ls END AS away_stint,
        CASE 
            WHEN 'FreeThrow' IN labels(s) THEN 1 
            WHEN '2PT' IN labels(s) THEN 2 
            WHEN '3PT' IN labels(s) THEN 3 
            ELSE 0 
        END AS points
    
    ORDER BY s.global_clock ASC, s.id ASC

    WITH g, 
        home.id AS home_id,
        collect(DISTINCT {
            period: p,
            home_stint: home_stint,
            away_stint: away_stint,
            shot: s,
            points: points,
            team_id: scoring_team.id
        }) AS events

    WITH g,
        reduce(
            acc = {
                chain: [],
                home_score: 0,
                away_score: 0,
                p_home_score: 0,
                p_away_score: 0,
                current_p: 0
            }, event IN events |
            CASE 
                WHEN event.period.n <> acc.current_p 
                THEN {
                    p_home_score: CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END,
                    p_away_score: CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END,
                    current_p: event.period.n,
                    
                    home_score: acc.home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END),
                    away_score: acc.away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END),
                    
                    chain: acc.chain + {
                        id: event.shot.id + "_score",
                        period: event.period,
                        shot: event.shot,
                        points: event.points,
                        home_stint: event.home_stint,
                        away_stint: event.away_stint,
                        scoring_team_id: event.team_id,
                        
                        home_score: acc.home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END),
                        away_score: acc.away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END),
                        margin: (acc.home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END)) - (acc.away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END)),
                        
                        period_home_score: CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END,
                        period_away_score: CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END,
                        period_margin: (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END) - (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END)
                    }
                }
                
                ELSE {
                    p_home_score: acc.p_home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END),
                    p_away_score: acc.p_away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END),
                    current_p: acc.current_p,
                    
                    home_score: acc.home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END),
                    away_score: acc.away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END),
                    
                    chain: acc.chain + {
                        id: event.shot.id + "_score",
                        period: event.period,
                        shot: event.shot,
                        points: event.points,
                        home_stint: event.home_stint,
                        away_stint: event.away_stint,
                        scoring_team_id: event.team_id,
                        
                        home_score: acc.home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END),
                        away_score: acc.away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END),
                        margin: (acc.home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END)) - (acc.away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END)),

                        period_home_score: acc.p_home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END),
                        period_away_score: acc.p_away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END),
                        period_margin: (acc.p_home_score + (CASE WHEN event.team_id = home_id THEN event.points ELSE 0 END)) - (acc.p_away_score + (CASE WHEN event.team_id <> home_id THEN event.points ELSE 0 END))
                    }
                }
            END
        ).chain AS calculated_chain

    UNWIND calculated_chain AS item
    WITH calculated_chain, item, 
        item.shot AS s, 
        item.home_stint AS hls,
        item.away_stint AS als
        
    
    MERGE (sc:Score {id: item.id})
    ON CREATE SET
        sc.home_score = item.home_score,
        sc.away_score = item.away_score,
        sc.margin = item.margin,
        sc.period_home_score = item.period_home_score,
        sc.period_away_score = item.period_away_score,
        sc.period_margin = item.period_margin,
        sc.global_clock = s.global_clock,
        sc.local_clock = s.local_clock,
        sc.time = s.time

    MERGE (s)-[:GENERATED_SCORE]->(sc)
    MERGE (sc)-[:AT_STINT]->(hls)
    MERGE (sc)-[:AT_STINT]->(als)

    WITH calculated_chain
    UNWIND range(0, size(calculated_chain)-2) AS i
    WITH 
        calculated_chain[i] AS current, 
        calculated_chain[i+1] AS next
    
    MATCH (c:Score {id: current.id})
    MATCH (n:Score {id: next.id})
    MERGE (c)-[:NEXT]->(n)
"""


SET_PLUS_MINUS = """
    MATCH (g:Game {id: $game_id})
    MATCH (ls:LineUpStint)-[:IN_PERIOD]->(:Period)-[:IN_GAME]->(g)
    OPTIONAL MATCH (ls)<-[:AT_STINT]-(sc:Score)<-[:GENERATED_SCORE]-(s:Shot)

    WITH ls, 
        CASE 
            WHEN s IS NULL THEN 0
            WHEN 'FreeThrow' IN labels(s) THEN 1
            WHEN '2PT' IN labels(s) THEN 2
            WHEN '3PT' IN labels(s) THEN 3
            ELSE 0 
        END AS points,
        EXISTS { (ls)<-[:ON_COURT_WITH]-(:PlayerStint)-[:TOOK_SHOT]->(s) } AS is_for

    WITH ls,
        sum(CASE WHEN is_for THEN points ELSE 0 END) AS ls_pf,
        sum(CASE WHEN NOT is_for AND points > 0 THEN points ELSE 0 END) AS ls_pa

    SET 
        ls.plus_minus = ls_pf - ls_pa,
        ls.points_scored = ls_pf,
        ls.points_conceded = ls_pa

    WITH ls 
    MATCH (ps:PlayerStint)-[:ON_COURT_WITH]->(ls)

    WITH ps, 
        sum(ls.plus_minus) AS ps_pm, 
        sum(ls.points_scored) AS ps_pf, 
        sum(ls.points_conceded) AS ps_pa

    SET     
        ps.plus_minus = ps_pm,
        ps.points_scored = ps_pf,
        ps.points_conceded = ps_pa
"""