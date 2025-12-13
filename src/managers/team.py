from ..manager import BaseManager
from ..queries.team import MERGE_TEAMS


class TeamManager(BaseManager):

    def load_teams(self):
        try: 
            teams_data = fetch_teams()
            params = {"teams": teams_data}
            result = self.execute_write(MERGE_TEAMS, params)
            print(f"{result}")

        except Exception as e:
            print(f" : {e}")
            return None
