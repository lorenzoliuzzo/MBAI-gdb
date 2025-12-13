---
title: Game Manager
layout: default
nav_order: 1
parent: Managers
---

# Game Manager

The `GameManager` in `src/managers/game.py` is responsible for game-related data.

## Key Features

- It inherits from `BaseManager`.
- It has methods to get teams for a game and to load a game's data, including periods and lineups, into the Neo4j database.
- It uses the `fetch_boxscore` and `fetch_pbp` functions from `fetcher.py` and queries from `src/queries/game.py`.

## Methods

- `get_teams(game_id)`: Retrieves the home and away team IDs for a given game.
- `load_game(game_id)`: Loads all data for a specific game, including periods, lineups, and play-by-play data.
- `load_periods(game_id, periods)`: Loads the period data for a game.
- `load_lineups(game_id, teams, subs, starters)`: Loads the lineup data for a game.
