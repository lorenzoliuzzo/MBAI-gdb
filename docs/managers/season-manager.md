---
title: Season Manager
layout: default
nav_order: 3
parent: Managers
---

# Season Manager

The `SeasonManager` in `src/managers/season.py` is responsible for loading the game schedule for a given season into the database.

## Key Features

- It inherits from `BaseManager`.
- It uses the `fetch_schedule` function from `fetcher.py` and the `MERGE_SEASON` query from `src/queries/season.py`.

## Methods

- `load_games(season_id)`: Loads the game schedule for a given season.
