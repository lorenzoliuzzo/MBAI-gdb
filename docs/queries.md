---
title: Queries
layout: default
nav_order: 5
has_children: true
---

# Queries

The `src/queries` package contains the Cypher logic used to interact with the Neo4j database. Rather than embedding SQL-like strings directly in Python code, we maintain them as modular constants.

## Core Logic
* [**Game Mechanics**](./game-mechanics.html): Detailed explanation of the *Stint Engine*, *Score Reconstruction*, and *Event Linking*.

## Source Files
| File | Description |
|:---|:---|
| `game.py` | Contains the massive queries for lineups (`MERGE_STINTS`), scoring (`MERGE_SCORES`), and actions (`MERGE_SHOTS`). |
| `season.py` | Handles the creation of the Season schedule and linking Games sequentially. |
| `team.py` | Manages static Team and Arena nodes. |
| `setup.py` | Defines database Constraints and Indexes ensuring uniqueness and performance. |