SETUP_QUERIES = [
    "CREATE CONSTRAINT team_id IF NOT EXISTS FOR (t:Team) REQUIRE t.id IS UNIQUE",
    "CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.id IS UNIQUE",
    "CREATE CONSTRAINT arena_name IF NOT EXISTS FOR (a:Arena) REQUIRE a.name IS UNIQUE",
    "CREATE CONSTRAINT city_name IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT state_name IF NOT EXISTS FOR (s:State) REQUIRE s.name IS UNIQUE",
    
    "CREATE CONSTRAINT season_id IF NOT EXISTS FOR (s:Season) REQUIRE s.id IS UNIQUE",

    "CREATE CONSTRAINT game_id IF NOT EXISTS FOR (g:Game) REQUIRE g.id IS UNIQUE",
    "CREATE INDEX IF NOT EXISTS FOR (g:Game) ON (g.date)",

    "CREATE CONSTRAINT period_id IF NOT EXISTS FOR (p:Period) REQUIRE p.id IS UNIQUE",
    
    "CREATE CONSTRAINT lineup_id IF NOT EXISTS FOR (l:LineUp) REQUIRE l.id IS UNIQUE",
    "CREATE CONSTRAINT lineup_stint_id IF NOT EXISTS FOR (ls:LineUpStint) REQUIRE ls.id IS UNIQUE",
    "CREATE CONSTRAINT player_stint_id IF NOT EXISTS FOR (ps:PlayerStint) REQUIRE ps.id IS UNIQUE",

    "CREATE CONSTRAINT action_id IF NOT EXISTS FOR (a:Action) REQUIRE a.id IS UNIQUE",
    "CREATE CONSTRAINT score_id IF NOT EXISTS FOR (s:Score) REQUIRE s.id IS UNIQUE",
    "CREATE CONSTRAINT margin_id IF NOT EXISTS FOR (m:ScoreMargin) REQUIRE m.id IS UNIQUE",

    "CREATE INDEX ls_timeline IF NOT EXISTS FOR (ls:LineUpStint) ON (ls.global_clock)",
    "CREATE INDEX ps_timeline IF NOT EXISTS FOR (ps:PlayerStint) ON (ps.global_clock)",
    "CREATE INDEX shot_timeline IF NOT EXISTS FOR (s:Action) ON (s.global_clock)", 
    "CREATE INDEX score_timeline IF NOT EXISTS FOR (s:Score) ON (s.global_clock)"
]