---
title: Team Manager
layout: default
nav_order: 4
parent: Managers
---

# Team Manager

The `TeamManager` in `src/managers/team.py` is responsible for loading team data into the database.

## Key Features

- It inherits from `BaseManager`.
- It uses the `fetch_teams` function from `fetcher.py` and the `MERGE_TEAMS` query from `src/queries/team.py`.

## Methods

- `load_teams()`: Loads all NBA teams into the database.
