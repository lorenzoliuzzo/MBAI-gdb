from ..manager import BaseManager
from ..queries.season import MERGE_SEASON
from ..fetcher import fetch_schedule


class SeasonManager(BaseManager):

    def load_games(self, season_id: str): 
        try:       
            data = fetch_schedule(season_id) 
            params = {"season_id": season_id, "schedule": data}
            result = self.execute_write(MERGE_SEASON, params)
            print(f"{result}")
            
        except Exception as e:
            print(f": {e}")
