---
title: Game Mechanics
layout: default
nav_order: 2
parent: Queries
---

# Game Mechanics
{:.no_toc}

This section details the complex Cypher algorithms used to reconstruct game flow. These queries are located in `src/queries/game.py` and are executed by the `GameManager`.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

---

## The Stint Engine (`MERGE_STINTS`)

The `MERGE_STINTS` query is the heart of the temporal model. It transforms raw substitution logs into a continuous timeline of on-court presence.

### The Algorithm
The query operates in three phases to bridge the gap between "Who is on the team" (Static) and "Who is playing right now" (Temporal).

1.  **LineUp Construction**: Creates `LineUp` nodes for every unique 5-man unit.
2.  **Temporal Instantiation**: Creates `LineUpStint` nodes representing that unit's specific time on the floor (e.g., Q1 12:00 to 08:30).
3.  **Player Aggregation**: Calculates `PlayerStint` nodes. If a player stays on the court while his teammates change, his `PlayerStint` will span multiple `LineUpStint`s.

### Architecture Diagram
The following diagram illustrates how static definitions (Teams/Lineups) instantiate into temporal nodes (Stints) and how individual player runs are aggregated.

```mermaid
graph TD
    %% Styling
    classDef static fill:#eee,stroke:#333,stroke-width:1px;
    classDef temporal fill:#d4f1f4,stroke:#00798c,stroke-width:2px;
    classDef player fill:#e2f0cb,stroke:#87a96b,stroke-width:2px;
    classDef team fill:#ffdd00,stroke:#333,stroke-width:2px;

    %% Layer 1: Static Definitions
    subgraph Static ["1. Static Definitions"]
        direction TB
        T[Team]:::team
        L[LineUp A]:::static
        L2[LineUp B]:::static
        P1[Player 1]:::static
        
        T -->|HAS_LINEUP| L & L2
        P1 -->|MEMBER_OF| L & L2
    end

    %% Layer 2: Temporal Instantiation
    subgraph Temporal ["2. Temporal Instantiation"]
        direction TB
        Per[Period 1]:::static
        
        LS1[LineUpStint A <br> 12:00 - 08:30]:::temporal
        LS2[LineUpStint B <br> 08:30 - 05:00]:::temporal
        
        LS1 -->|NEXT| LS2
        LS1 & LS2 -->|IN_PERIOD| Per
    end

    %% Layer 3: Player Aggregation
    subgraph Aggregation ["3. Player Runs"]
        PS1[PlayerStint <br> 12:00 - 05:00]:::player
    end

    %% Cross-Layer Connections
    L -->|ON_COURT| LS1
    L2 -->|ON_COURT| LS2

    P1 -->|ON_COURT| PS1
    
    %% Crucial Logic: One PlayerStint connects to multiple LineUpStints
    PS1 -->|ON_COURT_WITH| LS1
    PS1 -->|ON_COURT_WITH| LS2
    
    linkStyle 6,7,8 stroke:#ff9900,stroke-width:2px;

```

### Input Parameters (`$params`)

The query expects a nested structure representing the home and away sides:

```json
{
  "game_id": 22300001,
  "sides": [
    {
      "team_id": 1610612738,
      "lineups": [
        {
          "period": 1,
          "clock": "12:00",
          "global_clock": 0.0,
          "ids": [123, 456, 789, 101, 112]
        }
      ]
    }
  ]
}

```

---

## Event Processing

Events (Shots, Fouls, etc.) are anchored to the timeline using the `global_clock` property. We use specialized queries for each event type (e.g., `MERGE_SHOTS`, `MERGE_FOULS`) to attach specific properties.

### Context Linking

Every Action is linked to the **Context** (`LineUpStint`) and the **Actor** (`PlayerStint`) active at that exact moment.

```mermaid
graph LR
    PS[PlayerStint]
    LS[LineUpStint]
    Shot[Action:Shot]

    PS -->|ON_COURT_WITH| LS
    PS -->|TOOK_SHOT| Shot
    
    %% Implicit Time link
    subgraph Time ["Time Context"]
        LS
        Shot
    end
    
    style Time fill:#f9f9f9,stroke:#333,stroke-dasharray: 5 5

```

---

## Score Reconstruction (`MERGE_SCORES`)

Unlike simple box scores, MBAI-GDB reconstructs the score as a **Linked List of States**. The `MERGE_SCORES` query uses a Cypher `reduce()` function to iterate through all scoring events and calculate the running margin.

### The Score Chain

Each `Score` node represents the discrete state of the game *after* a specific shot.

```mermaid
graph LR
    classDef score fill:#f9f,stroke:#333,stroke-width:2px;
    classDef event fill:#fff,stroke:#333,stroke-dasharray: 5 5;

    S1[Score <br/> Home: 2 <br/> Away: 0]:::score
    S2[Score <br/> Home: 2 <br/> Away: 3]:::score
    S3[Score <br/> Home: 4 <br/> Away: 3]:::score

    Shot1[Shot:2PT]:::event
    Shot2[Shot:3PT]:::event
    Shot3[Shot:2PT]:::event

    Shot1 -->|GENERATED| S1
    S1 -->|NEXT| S2
    Shot2 -->|GENERATED| S2
    S2 -->|NEXT| S3
    Shot3 -->|GENERATED| S3

```

**Key Properties on `Score` Nodes:**

* `home_score`, `away_score`: Integer running totals.
* `margin`: (Home - Away).
* `period_margin`: The score differential within the specific period.

---

## Constraint Definitions (`setup.py`)

To ensure data integrity and query performance, the `BaseManager` enforces the following constraints on startup:

| Label | Property | Constraint Type | Purpose |
| --- | --- | --- | --- |
| `:Team`, `:Player`, `:Game` | `id` | **UNIQUE** | Prevents duplicate entity merging. |
| `:Action`, `:Score` | `id` | **UNIQUE** | Ensures idempotency of event loading. |
| `:LineUpStint`, `:PlayerStint` | `global_clock` | **INDEX** | Accelerates temporal range lookups. |