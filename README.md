# MBAI-gdb
**MBAI-gdb** is the graph database backend for the [Money Ball AI project](https://github.com/lorenzoliuzzo/MBAI). 

The goal is to transforms raw NBA play-by-play data into a high-fidelity [Neo4j](https://github.com/neo4j/neo4j) graph, enabling advanced analytics.
Unlike standard SQL approaches, this project models a game as a series of connected events and states.

## Core Entities
- `Team`: static entity representing a team.
  - Properties: 
    - id:
    - name:
    - abbreviation: 
    
- `Arena`: static entity representing an arena.
  - Properties: 
    - id:
  - Relationships: 
    - (:Team)-[:*HOME_ARENA*]->(:Arena)

- `Player`: static entity representing a player.
  - Properties: 
    - id:

- `LineUp`: static entity representing an unique combination of 5 `Player`.
  - Properties: 
    - id:
  - Relationships: 
    - (:Team)-[:*TEAM_LINEUP*]->(:LineUp)
    - (:Player)-[:*IN*]->(:LineUp)

- `Season`: static entity representing a season.
  - Properties: 
    - id:

- `Game`: the root node for a match.
  - Properties: 
    - date: 
    - start:
    - duration: 
  - Relationships: 
    - (:Game)->[:*IN*]->(:Season)
    - (:Team)-[:*PLAYED_HOME*|*PLAYED_AWAY*]->(:Game)
    - (:Game)-[:*IN*]->(:Arena)
    - (:Game)-[:*NEXT*]->(:Game)
    
- `Period`: a quarter of a game.
  - Properties:
    - n:  
    - start: 
    - duration:
  - Relationships: 
    - (:Period)-[:*IN*]->(:Game)
    - (:Period)-[:*NEXT*]->(:Period)

## The Timeline (Stints)
This part of the graph models "Who was on the court and for how long."
- `LineUpStint`: An instance of a LineUp playing in a specific period. 
  - Properties: 
    - clock_duration:
    - time_duration:
  - Relationships: 
    - (:LineUpStint)-[:*NEXT*]->(:LineUpStint). 
    
- `PlayerStint`: A contiguous block of time a single player is on the court. 
  - Properties: 
  - Relationships: 
    - (:PlayerStint)-[:*NEXT* {*time_since*: ..., *clock_since*: ...}]->(:PlayerStint). 
