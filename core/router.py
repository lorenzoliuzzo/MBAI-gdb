from pathlib import Path 


def get_base_path() -> Path:
    return Path("~").expanduser()

def get_data_path() -> Path:
    return get_base_path() / "MBAI/MBAI-gdb/data"

def get_season_path(season_id) -> Path:
    return get_data_path() / f"rs{season_id}"