---
layout: default
title: Period
parent: Architecture
has_children: true
nav_order: 4
---

# Period Architecture
The `Period` entity represents a distinct segment of game time (e.g., `Q1`, `Q2`, `OT`).

### Key Relationships:
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
    
    P1 -- NEXT {time_since} --> P2
    P2 -- NEXT {time_since} --> P3
    P3 -- NEXT {time_since} --> P4
    
    linkStyle default stroke:#ff9900,stroke-width:2px;
    style G fill:#f9f,stroke:#333
    style P1 fill:#bbf,stroke:#333
    style P2 fill:#bbf,stroke:#333
    style P3 fill:#bbf,stroke:#333
    style P4 fill:#bbf,stroke:#333
```