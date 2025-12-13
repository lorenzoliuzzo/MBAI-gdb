---
title: Managers
layout: default
nav_order: 4
has_children: true
---

# Managers

## Driver

The `src/driver.py` script is responsible for creating and managing the connection to the Neo4j graph database.

### Key Features

- **Singleton Pattern**: It uses a singleton pattern to ensure that only one instance of the Neo4j driver is created throughout the application's lifecycle. This is crucial for managing database connections efficiently.
- **Environment-Based Configuration**: The script loads database credentials (URI, username, and password) from a `.env` file. This is a good practice for keeping sensitive information out of the source code.
- **Connection Verification**: It verifies the connection to the database upon creation to ensure that the application can communicate with the database.
- **Connection Closing**: It provides a function to close the database connection gracefully.

### Functions

- `get_driver()`: This function returns the singleton instance of the Neo4j driver. If the driver has not been created yet, it will be created and a connection to the database will be established.
- `close_driver()`: This function closes the connection to the database.


## Base Manager

The `src/manager.py` script defines a `BaseManager` class that serves as the parent class for all domain-specific managers. It provides a layer of abstraction over the raw Neo4j database driver and handles the connection to the database.

### Key Features

- **Database Connection Handling**: It retrieves the Neo4j driver instance using the `get_driver()` function from `src/driver.py`.
- **Transaction Execution Methods**: It provides `execute_write` and `execute_read` methods for interacting with the database. These methods simplify the process of running Cypher queries and handling database sessions.

### Methods

- `execute_write(query, params)`: Executes a write transaction to the database.
- `execute_read(query, params)`: Executes a read transaction to the database and returns the results as a list of dictionaries.


# Domain Managers

The `src/managers` package contains domain-specific managers that inherit from the `BaseManager` class. These managers are responsible for handling the business logic related to their respective domains (e.g., games, players, seasons, teams).

Each manager uses the `execute_write` and `execute_read` methods from the `BaseManager` to interact with the database and execute Cypher queries defined in the `src/queries` package.
