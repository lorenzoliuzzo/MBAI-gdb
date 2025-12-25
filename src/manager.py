# core/manager.py

from typing import Any, Dict, List, Optional
from .driver import get_driver
from .queries.setup import SETUP_QUERIES

class BaseManager:
    """
    The parent class for all domain services. 
    Handles the driver reference and common transaction patterns.
    """
    def __init__(self):
        self.driver = get_driver()
        if self.driver is None:
            print("")
            raise Exception()

        with self.driver.session() as session:
            for query in SETUP_QUERIES:
                try:
                    session.run(query)
                except Exception as e:
                    print(f"Error creating constraint: {e}")


    def execute_write(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Runs a write transaction (creating/updating nodes).
        Automatically handles session creation and cleanup.
        """
        if params is None:
            params = {}
            
        with self.driver.session() as session:
            result = session.execute_write(
                lambda tx: tx.run(query, **params).consume()
            )
            return result


    def execute_read(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Runs a read transaction (fetching data).
        Returns a clean list of dictionaries (easier to use than raw Neo4j records).
        """
        if params is None:
            params = {}

        with self.driver.session() as session:
            result = session.execute_read(
                lambda tx: tx.run(query, **params).data()
            )
            return result