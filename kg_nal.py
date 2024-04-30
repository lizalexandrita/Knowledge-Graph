#!/usr/bin/env python
# coding: utf-8

# get_ipython().system('pip install neo4j')

import json, csv, re
import pandas as pd
from neo4j import GraphDatabase

# Connection to Neo4j
class Neo4jConnection:
    """
    Represents a connection to a Neo4j database.

    Args:
        uri (str): The URI of the Neo4j database.
        user (str): The username for authentication.
        pwd (str): The password for authentication.

    Attributes:
        __uri (str): The URI of the Neo4j database.
        __user (str): The username for authentication.
        __password (str): The password for authentication.
        __driver (neo4j.Driver): The Neo4j driver object.

    Methods:
        close(): Closes the connection to the Neo4j database.
        query(query, parameters=None, db=None): Executes a Cypher query on the Neo4j database.
        show_databases(): Retrieves a list of all databases in the Neo4j instance.
        delete_test_data(): Deletes all nodes with a 'test' property from the Neo4j database.
        delete_all_data(): Deletes all nodes and relationships from the Neo4j database.
        inspect_schema(): Retrieves the schema visualization of the Neo4j database.
        get_properties(entity_type, entity_label): Retrieves the properties of nodes, relationships, or constraints in the Neo4j database.

    Example usage:
        conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
        conn.query("MATCH (n) RETURN n LIMIT 10")
        conn.close()
    """

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__password = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        """
        Closes the connection to the Neo4j database.
        """
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        """
        Executes a Cypher query on the Neo4j database.

        Args:
            query (str): The Cypher query to execute.
            parameters (dict, optional): The parameters to pass to the query. Defaults to None.
            db (str, optional): The name of the database to execute the query on. Defaults to None.

        Returns:
            list: The result of the query as a list of records.

        Raises:
            AssertionError: If the driver is not initialized.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            result = conn.query("MATCH (n) RETURN n LIMIT 10")
            conn.close()
        """
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        return response

    def show_databases(self):
        """
        Retrieves a list of all databases in the Neo4j instance.

        Returns:
            list: The list of databases.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            databases = conn.show_databases()
            conn.close()
        """
        return self.query("SHOW DATABASES")

    def delete_test_data(self):
        """
        Deletes all nodes with a 'test' property from the Neo4j database.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            conn.delete_test_data()
            conn.close()
        """
        self.query("MATCH (n) WHERE n.test IS NOT NULL DETACH DELETE n")

    def delete_all_data(self):
        """
        Deletes all nodes and relationships from the Neo4j database.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            conn.delete_all_data()
            conn.close()
        """
        self.query("MATCH (n) DETACH DELETE n")

    def inspect_schema(self):
        """
        Retrieves the schema visualization of the Neo4j database.

        Returns:
            list: The schema visualization.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            schema = conn.inspect_schema()
            conn.close()
        """
        return self.query("CALL db.schema.visualization()")

    def get_properties(self, entity_type, entity_label):
        """
        Retrieves the properties of nodes, relationships, or constraints in the Neo4j database.

        Args:
            entity_type (str): The type of entity to retrieve properties for. Must be 'node', 'relationship', or 'constraint'.
            entity_label (str): The label of the entity to retrieve properties for.

        Raises:
            ValueError: If the entity_type is invalid.

        Example usage:
            conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
            conn.get_properties('node', 'Person')
            conn.close()
        """
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session() as session:
                if entity_type == 'node':
                    result = session.execute_read(self._get_all_node_properties, entity_label)
                elif entity_type == 'relationship':
                    result = session.execute_read(self._get_all_relationship_properties, entity_label)
                elif entity_type == 'constraint':
                    result = session.execute_read(self._get_all_constraints, entity_label)
                else:
                    raise ValueError(f"Invalid entity_type: {entity_type}. Must be 'node', 'relationship', or 'constraint'.")

                for record in result:
                    if record:
                        print(record)
                    else:
                        print("No properties found for the given relationship.")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def _get_all_node_properties(tx, node_label):
        query = f"""
        MATCH (n:{node_label})
        RETURN properties(n) AS properties
        """
        print(f"Executing query: {query}")  # Debugging print
        result = tx.run(query).data()
        print(f"Query result: {result}")  # Debugging print
        return result

    @staticmethod
    def _get_all_relationship_properties(tx, relationship_type):
        query = f"""
        MATCH ()-[r:{relationship_type}]->()
        RETURN type(r) AS type, properties(r) AS properties
        """
        print(f"Executing query: {query}")  # Debugging print
        result = tx.run(query).data()
        print(f"Query result: {result}")  # Debugging print
        return result

    @staticmethod
    def _get_all_constraints(tx, label):
        query = f"""
        SHOW CONSTRAINTS FOR (n:{label})
        """
        print(f"Executing query: {query}")  # Debugging print
        result = tx.run(query).data()
        print(f"Query result: {result}")  # Debugging print
        return result


# Graph functions
class GraphGenerator:
    """
    Generates nodes and relationships in a Neo4j database based on a provided schema and data.

    Args:
        neo4j_conn (Neo4jConnection): The Neo4j connection object.

    Attributes:
        neo4j_conn (Neo4jConnection): The Neo4j connection object.

    Methods:
        execute(schema, data): Generates nodes and relationships in the Neo4j database based on the provided schema and data.
        execute_from_json(json_path): Generates nodes and relationships in the Neo4j database based on a JSON file.

    Example usage:
        conn = Neo4jConnection("bolt://localhost:7687", "neo4j", "password")
        generator = GraphGenerator(conn)
        generator.execute(schema, data)
        generator.execute_from_json("data.json")
    """

    def __init__(self, neo4j_conn):
        self.neo4j_conn = neo4j_conn

    def execute(self, schema, data):
        """
        Generates nodes and relationships in the Neo4j database based on the provided schema and data.

        Args:
            schema (dict): A dictionary representing the schema of the nodes and relationships.
            data (dict): A dictionary containing the data for the nodes and relationships.

        Returns:
            None

        Raises:
            None

        Example usage:
            generator = GraphGenerator(conn)
            generator.execute(schema, data)
        """
        try:
            # self.generate_constraints(schema)
            self.generate_nodes(schema, data)
            self.generate_relationships(schema, data)
        except Exception as e:
            print("Execution had an error: ", e)

    def execute_from_json(self, json_path):
        """
        Generates nodes and relationships in the Neo4j database based on a JSON file.

        Args:
            json_path (str): The path to the JSON file containing the data.

        Returns:
            None

        Raises:
            None

        Example usage:
            generator = GraphGenerator(conn)
            generator.execute_from_json("data.json")
        """
        try:
            self.generate_from_json(json_path)
        except Exception as e:
            print("Execution had an error: ", e)

    def generate_from_json(self, json_path):
        """
        Generates nodes and relationships in the Neo4j database based on a JSON file.

        Args:
            json_path (str): The path to the JSON file containing the data.

        Returns:
            None

        Raises:
            None

        Example usage:
            generator = GraphGenerator(conn)
            generator.generate_from_json("data.json")
        """
        # Load the JSON data
        with open(json_path, 'r') as file:
            data = json.load(file)

        nodes = data['nodes']
        relationships = data['relationships']

        # Generate Nodes using MERGE
        for node in nodes:
            labels = node['labels'][0]  # Assuming only the first node label
            unique_identifier_key = 'name'  # Adjust if your unique identifier key is different

            # Ensure there's a unique identifier for the node. If not, skip or handle accordingly.
            if unique_identifier_key not in node['properties']:
                print(f"Skipping node without unique identifier: {node}")
                continue

            properties = node['properties']

            # Preparing the parameters for the query
            parameters = {prop: properties[prop] for prop in properties}

            # The MERGE statement with placeholders for the unique identifier and properties
            merge_query = f"""
                MERGE (n:{labels} {{name: $name}})
                ON CREATE SET n += $properties
                ON MATCH SET n += $properties
                RETURN n
            """
            # Execute the MERGE query with parameters
            #print(parameters)
            self.neo4j_conn.query(merge_query, parameters={'name': properties.get('name'), 'properties': parameters})

        # Generate Relationships using MERGE
        for rel in relationships:
            # Extract 'from' and 'to' IDs for source and target nodes and relationship type
            source_id = rel['from']
            target_id = rel['to']
            rel_type = rel['type']

            # Assuming relationships could have properties.
            # Here's a placeholder to construct a properties string if they existed.
            #rel_properties = {}  # Placeholder for relationship properties if any
            #set_clause_str = ", ".join([f"r.{key} = ${key}" for key in rel_properties.keys()])

            # If there are no properties to set, the SET clauses should be omitted.
            #on_create_set_clause = f"ON CREATE SET {set_clause_str}" if set_clause_str else ""
            #on_match_set_clause = f"ON MATCH SET {set_clause_str}" if set_clause_str else ""

            # Constructing and executing the MERGE query
            cypher_query = (
                f"""MATCH (a), (b)
                WHERE a.name = "{source_id}" AND b.name = "{target_id}"
                MERGE (a)-[r:{rel_type}]->(b)
                RETURN a, r, b
                """
            )
                #f"{on_create_set_clause} "
                #f"{on_match_set_clause} "
            self.neo4j_conn.query(cypher_query)
        print("Nodes and relationships have been created from JSON.")

    def generate_nodes(self, schema, data):
        """
        Generates nodes in a Neo4j database based on the provided schema and data.

        Args:
            schema (dict): A dictionary representing the schema of the nodes.
            data (dict): A dictionary containing the data for the nodes.

        Returns:
            None

        Raises:
            None

        Example usage:
            generator = GraphGenerator(conn)
            generator.generate_nodes(schema, data)
        """ 
        for node in schema['nodes']:
            node_label = node['labels'][0]  # Assuming each node dictionary has a 'labels' list with at least one label
            node_properties = node['properties']
            for node_data in data.get(node_label, []):
                # Construct a dictionary of property assignments for Cypher query
                node_props = ', '.join([f"{prop}: ${prop}" for prop in node_properties])
                # Assuming 'id' or another unique identifier is part of node_data to distinguish nodes
                node_id = node_data.get('id')  
                if node_id:  # Ensuring there is an identifier to match nodes in the database
                    # Use MERGE to create or update nodes based on unique identifier
                    cypher_query = f"MERGE (n:{node_label} {{id: $id, {node_props} }})"
                    # Assuming node_data is a dictionary with property values, including the 'id'
                    self.neo4j_conn.query(cypher_query, parameters=node_data)

    def merge_node_from_dict(self, node_label: str, node_dict: dict={}):
        """
        Creates a node in the Neo4j database from a dictionary.

        Args:
            node_label (str): The label of the node to create.
            node_dict (dict): A dictionary containing the node properties.

        Returns:
            None
        """
        try:
            self.neo4j_conn.query(
                f"""
                MERGE (n:{node_label} {{id: $id}})
                SET n += $props
                """,
                {"id": node_dict["id"], "props": node_dict},
            )
        except Exception as e:
            print("Execution had an error: ", e)
        

    def merge_relationship_from_node_to_node_by_id(self, from_node_id: str, to_node_id: str, rel_type: str, rel_props: dict={}):
        """
        Creates a relationship between two nodes in the Neo4j database by type.

        Args:
            tx (Transaction): The Neo4j transaction object.
            from_node_id (str): The ID of the source node.
            to_node_id (str): The ID of the target node.
            rel_type (str): The type of the relationship.
            rel_props (dict): A dictionary containing the relationship properties.

        Returns:
            None
        """
        try:
            self.neo4j_conn.query(
                """
                MATCH (a), (b)
                WHERE a.id = $from_node_id AND b.id = $to_node_id
                MERGE (a)-[r:%s]->(b)
                SET r += $props
                """ % rel_type,
                {"from_node_id": from_node_id, "to_node_id": to_node_id, "props": rel_props},
            )
        except Exception as e:
            print("Execution had an error: ", e)

    
    def merge_relationship_from_node_to_node_by_property(self, from_node_label: str, to_node_label: str, from_property_name: str, to_property_name: str, rel_type: str, rel_props: dict):
        """
        Creates a relationship between two nodes in the Neo4j database based on a common property.

        Args:
            tx (Transaction): The Neo4j transaction object.
            from_node_label (str): The label of the source node.
            to_node_label (str): The label of the target node.
            from_property_name (str): The name of the property in the source node.
            to_property_name (str): The name of the property in the target node.
            rel_type (str): The type of the relationship.
            rel_props (dict): A dictionary containing the relationship properties.

        Returns:
            None
        """
        try:
            self.neo4j_conn.query(
                f"""
                MATCH (a:{from_node_label}), (b:{to_node_label})
                WHERE a.{from_property_name} = b.{to_property_name}
                MERGE (a)-[r:{rel_type}]->(b)
                SET r += $props
                """,
                props=rel_props,
            )
        except Exception as e:
            print("Execution had an error: ", e)

    

    def generate_constraints(self, schema):
        for constraint_name, constraint_data in schema['constraints'].items():
            label = constraint_data['label']
            property_name = constraint_data['property']
            cypher_query = f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_name} IS UNIQUE;"
            self.neo4j_conn.query(cypher_query)


# Data Parsing
class ParseData:

    def extract_node_from_json(file_path: str, n: int = None) -> list[dict]:
        """
        Extracts data of a node from a JSON file and returns a list of dictionaries.

        Args:
            file_path (str): The file path for the JSON data.
            n (int, optional): Number of items to process. If None, all items are processed.

        Returns:
            list[dict]: A list of dictionaries containing the processed data.
        """
        with open(file_path, "r") as file:
            data = json.load(file)

        return data[:n] if n is not None else data

    # Extracting test.json Schema
    # TODO: ADAPT TO EXTRACT SCHEMA FROM BOTH JSON FROM STAN AND JAY
    def extract_schema_and_data_from_json(json_file_path):
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        schema = {
            "nodes": [],
            "relationships": []  # Assuming a similar structure for relationships
        }
        data = {
            "nodes": [],
            "relationships": []  # Initialize as a list
        }

        # Extracting nodes and their properties
        for node in json_data.get("nodes", []):  # Iterate through the list of nodes
            node_label = node['labels'][0]  # Assuming each node has at least one label
            if node_label not in schema["nodes"]:
                schema["nodes"].append(node_label)
            node_properties = node['properties']
            data["nodes"].append({  # Correct usage of append for a list
                "label": node_label,
                "properties": node_properties
            })

        # Extracting relationships and their properties
        for relationship in json_data.get("relationships", []):  # Iterate through the list of relationships
            from_node = relationship['from']
            to_node = relationship['to']
            relationship_type = relationship['type']

            # Append the relationship dictionary to the data["relationships"] list
            data["relationships"].append({
                "from": from_node,
                "to": to_node,
                "type": relationship_type
                # Add more properties here if needed
            })

        return schema, data


    # Extracting schema from jay_data_schema
    def extract_schema(json_data):
        schema = {
            'nodes': {},
            'relationships': []
        }

        for node in json_data['nodes']:
            label = node['labels'][0]  # Assuming one label per node
            schema['nodes'][label] = node['properties'].keys()

        for rel in json_data['relationships']:
            # Assuming 'type' is provided in the relationships
            if 'type' in rel and rel['type']:
                schema['relationships'].append({
                    'type': rel['type'],
                    'from': rel['fromId'],  # we can turn into ID again, my issue was readability for QA
                    'to': rel['toId'],
                    'properties': rel['properties'].keys()
                })

        return schema

    # Extracting data from jay_data
    def extract_csv_headers(csv_file_path):
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Read the first line to get the headers
            for header in headers:
                header = re.sub(r'[^\w]', '_', header)
        return headers

