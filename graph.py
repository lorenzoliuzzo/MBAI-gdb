GET_TEAM_IDS = """
    MATCH (g:Game {id: $game_id})-[:AT]->(a:Arena)
    MATCH (ht:Team)-[:HOME_ARENA]->(a)
    MATCH (ht)-[:PLAYED_HOME]->(g)
    MATCH (at:Team)-[:PLAYED_AWAY]->(g)
    RETURN ht.id, at.id LIMIT 1 
"""


def get_teams(session, game_id): 
    result = session.execute_read(lambda tx: 
        tx.run(GET_TEAM_IDS, game_id=game_id).single()
    )
        
    # if not result:
    #     print(f"{game_id}.")

    return result["ht.id"], result["at.id"]