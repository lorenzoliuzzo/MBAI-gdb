GET_TEAM_IDS = """
    MATCH (ht:Team)-[:PLAYS_HOME]->(g:Game {id: $game_id})<-[:PLAYS_AWAY]-(at:Team)
    RETURN ht.id, at.id
"""

def get_teams(session, game_id): 
    result = session.execute_read(lambda tx: 
        tx.run(GET_TEAM_IDS, game_id=game_id).single()
    )
        
    # if not result:
    #     print(f"{game_id}.")

    return result["ht.id"], result["at.id"]