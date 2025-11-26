import os
from dotenv import load_dotenv
from neo4j import GraphDatabase


def get_driver():
    try:
        load_dotenv()
        URI = os.getenv("NEO4J_URI")
        USERNAME = os.getenv("NEO4J_USERNAME")
        PASSWORD = os.getenv("NEO4J_PASSWORD")
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        driver.verify_connectivity()
    
    except Exception as e:
        print(f"Failed to create Neo4j driver: {e}")
        return
    
    return driver