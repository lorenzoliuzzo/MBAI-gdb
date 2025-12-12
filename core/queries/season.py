# core/queries/season.py

MERGE_SEASON = """
    MERGE (s:Season {id: $season_id})  
    WITH s
    UNWIND $schedule AS game

    MERGE (g:Game {id: game.game_id})-[:IN_SEASON]->(s)
    SET g.date = datetime(game.datetime)

    WITH s, game, g
    MATCH (ht:Team {id: game.home_team_id})-[:HOME_ARENA]->(a:Arena)
    MATCH (at:Team {id: game.away_team_id})
    MERGE (g)-[:AT]->(a)
    MERGE (ht)-[:PLAYED_HOME]->(g)
    MERGE (at)-[:PLAYED_AWAY]->(g)

    WITH distinct s
    MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(g:Game)-[r:IN_SEASON]->(s)
    WITH t, g ORDER BY g.date ASC

    WITH t, collect(g) AS games
    UNWIND range(0, size(games) - 2) AS i

    WITH games[i] AS current, games[i+1] AS next
    MERGE (current)-[r:NEXT]->(next)
    SET r.time_since = duration.between(current.date, next.date)
"""