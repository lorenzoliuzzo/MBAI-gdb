import os
from dotenv import load_dotenv
from typing import List, Dict, Any
from neo4j import GraphDatabase, Driver


class Neo4jHandler:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver.verify_connectivity()
    
    def close(self):
        self.driver.close()
    
    def execute_query(self, query: str, parameters: Dict = None) -> List[Any]:
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record for record in result]
    
    def execute_write_query(self, query: str, parameters: Dict = None) -> List[Any]:
        with self.driver.session() as session:
            result = session.write_transaction(
                lambda tx: tx.run(query, parameters or {})
            )
            return [record for record in result]


def get_driver():
    try:
        load_dotenv()
        URI = os.getenv("NEO4J_URI")
        AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        driver = GraphDatabase.driver(URI, auth=AUTH)
        driver.verify_connectivity()
    
    except Exception as e:
        print(f"Failed to create Neo4j driver: {e}")
        return

    return driver