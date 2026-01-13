# MBAI-gdb
**MBAI-gdb** is the graph database backend for the [Money Ball AI project](https://github.com/lorenzoliuzzo/MBAI). 

**MBAI-GDB** is an advanced graph ingestion engine that transforms raw, tabular NBA play-by-play data into a high-fidelity **Heterogeneous Temporal Graph** stored in [Neo4j](https://github.com/neo4j/neo4j).

Traditional sports analytics often rely on aggregated box scores (e.g., relational tables). *MBAI-GDB* breaks this paradigm by modeling basketball as a complex network of interactions. It parses thousands of events per game — shots, assists, fouls, and substitutions — into distinct nodes, linking them temporally via `NEXT` relationships.

Please, take a look at the [documentation](https://lorenzoliuzzo.github.io/MBAI-gdb/).