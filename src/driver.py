# core/driver.py
import os
from dotenv import load_dotenv
from threading import Lock
from neo4j import GraphDatabase


_driver = None
_driver_lock = Lock()


def get_driver():
    global _driver
    
    if _driver:
        return _driver

    with _driver_lock:
        try:
            load_dotenv()
            URI = os.getenv("NEO4J_URI")
            USERNAME = os.getenv("NEO4J_USERNAME")
            PASSWORD = os.getenv("NEO4J_PASSWORD")
            _driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
            _driver.verify_connectivity()
            print("Connected to Neo4j (Singleton Created)")
        
        except Exception as e:
            print(f"❌ Failed to connect to Neo4j: {e}")
            return None
                
        return _driver


def close_driver():
    global _driver
    with _driver_lock:
        if _driver:
            _driver.close()
            _driver = None
            print("🔌 Neo4j Connection Closed")