# MBAI-gdb
**MBAI-gdb** is the graph database backend for the [Money Ball AI project](https://github.com/lorenzoliuzzo/MBAI). 

The goal is to transforms raw NBA play-by-play data into a high-fidelity [Neo4j](https://github.com/neo4j/neo4j) graph, enabling advanced analytics.

# Basketball Graph Data Model

This database models basketball games as a temporal graph, connecting static entities (Players, Teams) with high-resolution chronological events (Stints) to allow for advanced lineup efficiency and player rotation analysis.

## Core Entities
- `Team`: static entity representing a team.
	- Properties: 
		- `id`: unique integer.
		- `name`: unique string.
		- `abbreviation`: unique string.
		
- `Arena`: static entity representing an arena.
	- Properties: 
		- `name`: unique string.
	- Relationships: 
		- (:`Team`)-[:`HOME_ARENA`]->(:`Arena`)

- `City`
	- Properties: 
		- `name`: unique string.
	- Relationships: 
		- (:`Arena`)-[:`IN`]->(:`City`)

- `State`
	- Properties: 
		- `name`: unique string.
  - Relationships: 
  	- (:`City`)-[:`IN`]->(:`State`)

- `Player`: static entity representing a player.
  - Properties: 
    - `id`: unique integer. 

- `LineUp`: static entity representing an unique combination of 5 `Player`.
  - Properties: 
    - `id`: unique string obtained composing the players id sorted in ascending order.
  - Relationships: 
    - (:`Team`)-[:`TEAM_LINEUP`]->(:`LineUp`)
    - (:`Player`)-[:`IN`]->(:`LineUp`)

- `Season`: static entity representing a season.
  - Properties: 
    - `id`: unique string (i.e. "2024-25").

- `Game`: the root node for a match.
  - Properties: 
    - `date`: datetime.
    - `start`: datetime.
    - `duration`: duration between first period start and last period end.
  - Relationships: 
    - (:`Game`)-[:`IN`]->(:`Season`)
    - (:`Team`)-[:`PLAYED_HOME`|`PLAYED_AWAY`]->(:`Game`)
    - (:`Game`)-[:`AT`]->(:`Arena`)
    - (:`Game`)-[:`NEXT` {team_id: ..., time_since: ...}]->(:`Game`)
    
- `Period`: a quarter of a game.
  - Properties:
    - `n`: integer identifier. 
    - `start`: datetime.
    - `duration`: duration between period start and period end.
  - Relationships: 
    - (:`Period`)-[:`IN`]->(:`Game`)
    - (:`Period`)-[:`NEXT` {time_since: ...}]->(:`Period`)

## The Timeline (Stints)
This part of the graph models "Who was on the court and for how long."
- `LineUpStint`: An instance of a LineUp playing in a specific period. 
  - Properties: 
    - `id`: unique string obtained composing the lineup id and 
    - `clock`: duration 
    - `global_clock`: integer representing the seconds elapsed from game start. 
    - `clock_duration`: duration between 
    - `time`: datetime 
    - `time_duration`: duration between 
  - Relationships: 
    - (:`LineUp`)-[:`HAD_STINT`]->(:`LineUpStint`): 
    - (:`LineUpStint`)-[:`VS` {`clock_duration`: ..., `time_duration`: ...}]->(:`LineUpStint`): 
    - (:`LineUpStint`)-[:`NEXT`]->(:`LineUpStint`): 
    
- `PlayerStint`: A contiguous block of time a single player is on the court. 
  - Properties: 
    - `id`: unique integer obtained composing the player id and 
    - `clock`: duration 
    - `global_clock`: integer representing the seconds elapsed from game start.
    - `clock_duration`: duration between
    - `time`: datetime
    - `time_duration`: duration between
  - Relationships: 
    - (:`Player`)-[:`HAD_STINT`]->(:`PlayerStint`): 
    - (:`PlayerStint`)-[:`IN`]->(:`LineUpStint`):
    - (:`PlayerStint`)-[:`VS` {`clock_duration`: ..., `time_duration`: ...}]->(:`LineUpStint`): 
    - (:`PlayerStint`)-[:`NEXT` {`clock_since`: ..., `time_since`: ...}]->(:`PlayerStint`): 
