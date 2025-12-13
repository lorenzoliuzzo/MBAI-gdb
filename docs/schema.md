---
title: Schema
layout: default
nav_order: 3
has_children: true
---

# Schema

This section provides an overview of the technical architecture of the MBAI-gdb project. It covers the main components of the system and how they interact with each other.




## `Game` architecture
```mermaid
graph LR
    linkStyle default stroke:#ff9900,stroke-width:2px;

    S[Season]
    style S fill:#f3f,stroke:#333

    G[Game]
    style G fill:#f9f,stroke:#333

    ht[Team]
    at[Team]
    style ht fill:#ff2,stroke:#333
    style at fill:#ff2,stroke:#333

    a[Arena]
    style a fill:#bbf,stroke:#333

    G -- IN_SEASON --> S
    G -- AT {date} --> a 
    ht -- PLAYED_HOME --> G
    at -- PLAYED_AWAY --> G
    ht -- HOME_ARENA --> a

    linkStyle 2 stroke:green,stroke-width:3px;
    linkStyle 3 stroke:red,stroke-width:3px;
```

### Time Chain with `NEXT`
```mermaid
graph LR
    G[Game]
    style G fill:#f9f,stroke:#333

    G1[Game]
    style G1 fill:#f9f,stroke:#333

    G2[Game]
    style G2 fill:#f9f,stroke:#333

    ht[Team]
    style ht fill:#ff2,stroke:#333

    at[Team]
    style at fill:#ff2,stroke:#333

    at1[Team]
    style at1 fill:#ff2,stroke:#333

    at2[Team]
    style at2 fill:#ff2,stroke:#333

    G -- NEXT {since} --> G1
    G -- NEXT {since} --> G2

    ht -- PLAYED_HOME --> G
    at -- PLAYED_AWAY --> G

    ht -- PLAYED_HOME --> G1
    at1 -- PLAYED_AWAY --> G1

    at -- PLAYED_HOME --> G2
    at2 -- PLAYED_AWAY --> G2

    linkStyle 0,1 stroke:blue,stroke-width:3px;
    linkStyle 2,4,6 stroke:green,stroke-width:3px;
    linkStyle 3,5,7 stroke:red,stroke-width:3px;
```


### `Period` architecture
The `Period` entity represents a distinct segment of game time (e.g., `Q1`, `Q2`, `OT`).

#### Key Relationships:
- `Period`s are linked sequentially via [:`NEXT`]. This *time chain* allows us to traverse the game from start to finish linearly.
- Every `Period` connects to the `Game` via [:`IN_GAME`].
- Labels like :`RegularTime`:`Q1` or :`OverTime` for easy filtering

```mermaid
graph LR
    G[Game]
    P1[Q1]
    P2[Q2]
    P3[Q3]
    P4[Q4]
    
    P1 -- IN_GAME --> G
    P2 -- IN_GAME --> G
    P3 -- IN_GAME --> G
    P4 -- IN_GAME --> G
    
    P1 -- NEXT {since} --> P2
    P2 -- NEXT {since} --> P3
    P3 -- NEXT {since} --> P4
    
    linkStyle default stroke:#ff9900,stroke-width:2px;
    linkStyle 4,5,6 stroke:blue,stroke-width:3px;
    style G fill:#f9f,stroke:#333
    style P1 fill:#bbf,stroke:#333
    style P2 fill:#bbf,stroke:#333
    style P3 fill:#bbf,stroke:#333
    style P4 fill:#bbf,stroke:#333
```


```mermaid
graph TB
    T[Team]
    style T fill:#ff2,stroke:#333
    
    L[LineUp]
    style L fill:#f9f,stroke:#333
    
    P1[Player 1]
    P2[Player 2]
    P3[Player 3]
    P4[Player 4]
    P5[Player 5]

    T -->|HAS_LINEUP| L
    P1 & P2 & P3 & P4 & P5 -->|MEMBER_OF| L
```


```mermaid
graph TB
    T[Team]
    style T fill:#ff2,stroke:#333

    L[LineUp]
    style L fill:#f9f,stroke:#333

    T -->|HAS_LINEUP| L

    LS[LineUpStint]
    style LS fill:#bbf,stroke:#333
    
    P1[Player 1]
    P2[Player 2]
    P3[Player 3]
    P4[Player 4]
    P5[Player 5]

    PS1[PlayerStint]
    PS2[PlayerStint]
    PS3[PlayerStint]
    PS4[PlayerStint]
    PS5[PlayerStint]

    L -->|HAD_STINT| LS
    P1 & P2 & P3 & P4 & P5 -->|MEMBER_OF| L
    PS1 & PS2 & PS3 & PS4 & PS5 -->|APPEARED_IN| LS
    P1 -->|HAD_STINT| PS1
    P2 -->|HAD_STINT| PS2
    P3 -->|HAD_STINT| PS3
    P4 -->|HAD_STINT| PS4
    P5 -->|HAD_STINT| PS5
```

```mermaid
graph LR
    T[Team]
    style T fill:#ff2,stroke:#333
    
    L1[LineUp]
    style L1 fill:#f9f,stroke:#333

    L2[LineUp]
    style L2 fill:#f9f,stroke:#333
    
    L3[LineUp]
    style L3 fill:#f9f,stroke:#333


    LS1[LineUpStint]
    style LS1 fill:#bbf,stroke:#333
    
    LS2[LineUpStint]
    style LS2 fill:#bbf,stroke:#333

    LS3[LineUpStint]
    style LS3 fill:#bbf,stroke:#333

    LS4[LineUpStint]
    style LS4 fill:#bbf,stroke:#333

    q1[Q1]
    style q1 fill:#eee,stroke:#333

    q2[Q2]
    style q2 fill:#eee,stroke:#333

    T -->|HAS_LINEUP| L1 & L2 & L3
    
    L1 -->|HAD_STINT| LS1
    LS1 -->|IN_PERIOD| q1

    L2 -->|HAD_STINT| LS2 & LS3
    LS2 -->|IN_PERIOD| q1

    LS1 -- NEXT {duration} --> LS2

    L3 -->|HAD_STINT| LS4
    LS3 -->|IN_PERIOD| q2
    LS4 -->|IN_PERIOD| q2

    LS3 -- NEXT {duration} --> LS4
    linkStyle 11 stroke:blue,stroke-width:3px;
```