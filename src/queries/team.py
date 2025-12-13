# core/queries/team.py

MERGE_TEAMS = """
    UNWIND $teams AS team 
    MERGE (t:Team {id: team.id})
    ON CREATE SET
        t.name = team.full_name,
        t.abbreviation = team.abbreviation,
        t.city = team.city,
        t.state = team.state
        
    MERGE (t)-[:HOME_ARENA]->(a:Arena {name: team.arena})
"""