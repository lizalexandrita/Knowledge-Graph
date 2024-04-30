# Knowledge Graph

This repository contains the following scripts:

## kg_nal.py

The `kg_nal.py` script provides a Python interface for interacting with a Neo4j database. It includes two main classes: `Neo4jConnection` and `GraphGenerator`.

### Neo4jConnection

The `Neo4jConnection` class represents a connection to a Neo4j database. It takes the following arguments during initialization:

- `uri` (str): The URI of the Neo4j database.
- `user` (str): The username for authentication.
- `pwd` (str): The password for authentication.

The `Neo4jConnection` class has the following attributes:

- `__uri` (str): The URI of the Neo4j database.
- `__user` (str): The username for authentication.
- `__password` (str): The password for authentication.
- `__driver` (neo4j.Driver): The Neo4j driver object.

The `Neo4jConnection` class provides the following methods:

- `close()`: Closes the connection to the Neo4j database.
- `query(query, parameters=None, db=None)`: Executes a Cypher query on the Neo4j database.
- `show_databases()`: Retrieves a list of all databases in the Neo4j instance.
- `delete_test_data()`: Deletes all nodes with a 'test' property from the Neo4j database.
- `delete_all_data()`: Deletes all nodes and relationships from the Neo4j database.
- `inspect_schema()`: Retrieves the schema visualization of the Neo4j database.
- `get_properties(entity_type, entity_label)`: Retrieves the properties of nodes, relationships, or constraints in the Neo4j database.

### GraphGenerator

The `GraphGenerator` class generates nodes and relationships in a Neo4j database based on a provided schema and data. It takes a `Neo4jConnection` object as an argument during initialization.

The `GraphGenerator` class has the following attributes:

- `neo4j_conn` (Neo4jConnection): The Neo4j connection object.

The `GraphGenerator` class provides the following methods:

- `execute(schema, data)`: Generates nodes and relationships in the Neo4j database based on the provided schema and data.
- `execute_from_json(json_path)`: Generates nodes and relationships in the Neo4j database based on a JSON file.

Here is an example usage of the `Neo4jConnection` and `GraphGenerator` classes:

## extract_from_ipynb.py

The `extract_from_ipynb.py` script provides a method to extract Python code from a Jupyter notebook.

### extract_python_code_from_notebook

The `extract_python_code_from_notebook` function takes a notebook file path as input and returns the Python code extracted from the notebook. It uses the `nbformat` library to read the notebook file and filters out code cells to concatenate their content.

Here is an example usage of the `extract_python_code_from_notebook` function:

## create_neo4j_configs.py

The `create_neo4j_configs.py` script provides a method to create a Neo4j configuration file with the specified URI, username, and password.

### create_neo4j_configs

The `create_neo4j_configs` function creates a Neo4j configuration file with the specified URI, username, and password.
