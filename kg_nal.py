#!/usr/bin/env python
# coding: utf-8

# get_ipython().system('pip install neo4j')

import json, csv, re
import pandas as pd
from neo4j import GraphDatabase

# Connection to Neo4j
class Neo4jConnection:
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
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
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
        return self.query("SHOW DATABASES")

    def delete_test_data(self):
        self.query("MATCH (n) WHERE n.test IS NOT NULL DETACH DELETE n")

    def delete_all_data(self):
        self.query("MATCH (n) DETACH DELETE n")

    def inspect_schema(self):
        return self.query("CALL db.schema.visualization()")

    def get_properties(self, entity_type, entity_label):
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
    def __init__(self, neo4j_conn):
        self.neo4j_conn = neo4j_conn

    def execute(self, schema, data):
        try:
            # self.generate_constraints(schema)
            self.generate_nodes(schema, data)
            self.generate_relationships(schema, data)
        except Exception as e:
            print("Execution had an error: ", e)

    def execute_from_json(self, json_path):
        try:
            self.generate_from_json(json_path)
        except Exception as e:
            print("Execution had an error: ", e)

    def generate_from_json(self, json_path):
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
            schema (dict): A dictionary representing the schema of the nodes. It should have the following structure:
                {
                    'nodes': [
                        {
                            'labels': ['label1', 'label2', ...],  # A list of labels for the node
                            'properties': ['prop1', 'prop2', ...]  # A list of properties for the node
                        },
                        ...
                    ]
                }
            data (dict): A dictionary containing the data for the nodes. It should have the following structure:
                {
                    'label1': [
                        {
                            'id': 'node_id1',  # A unique identifier for the node
                            'prop1': 'value1',  # Property values for the node
                            'prop2': 'value2',
                            ...
                        },
                        ...
                    ],
                    'label2': [
                        ...
                    ],
                    ...
                }

        Returns:
            None

        Raises:
            None

        Example usage:
            schema = {
                'nodes': [
                    {
                        'labels': ['Person'],
                        'properties': ['name', 'age']
                    },
                    {
                        'labels': ['Company'],
                        'properties': ['name', 'location']
                    }
                ]
            }

            data = {
                'Person': [
                    {
                        'id': 'person1',
                        'name': 'John Doe',
                        'age': 30
                    },
                    {
                        'id': 'person2',
                        'name': 'Jane Smith',
                        'age': 25
                    }
                ],
                'Company': [
                    {
                        'id': 'company1',
                        'name': 'ABC Corp',
                        'location': 'New York'
                    }
                ]
            }

            generate_nodes(schema, data)
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

    def create_node_from_dict(self, tx, node_label: str, node_dict: dict):
        """
        Creates a node in the Neo4j database from a dictionary.

        Args:
            tx (Transaction): The Neo4j transaction object.
            node_label (str): The label of the node to create.
            node_dict (dict): A dictionary containing the node properties.

        Returns:
            dict: A dictionary containing the number of nodes created, relationships created, and nodes matched.
            Example:
            {
                'nodes_created': 10,
                'relationships_created': 5,
                'nodes_matched': 3
            }
        """
        try:
            tx.run(
                f"""
                MERGE (n:{node_label} {{id: $id}})
                SET n += $props
                """,
                id=node_dict["id"],
                props=node_dict,
            )
        except Exception as e:
            print("Execution had an error: ", e)
        
        result = self.neo4j_conn.query("""
            MATCH (n)
            RETURN count(n) AS nodes_created
        """).single()
        nodes_created = result['nodes_created']

        result = self.neo4j_conn.query("""
            MATCH ()-[r]->()
            RETURN count(r) AS relationships_created
        """).single()
        relationships_created = result['relationships_created']

        result = self.neo4j_conn.query("""
            MATCH (n)
            RETURN count(n) AS nodes_matched
        """).single()
        nodes_matched = result['nodes_matched']

        return {
            'nodes_created': nodes_created,
            'relationships_created': relationships_created,
            'nodes_matched': nodes_matched
        }

    def generate_relationships(self, schema, data):
        for edge_type, edge_properties in schema['relationships'].items():
            for edge_data in data.get(edge_type, []):
                source_id = edge_data['from']
                target_id = edge_data['to']
                set_clauses = []
                for prop, value in edge_data.items():
                    if prop not in ["from", "to"]:  # Exclude the source and target properties
                        set_clauses.append(f"r.{prop} = ${prop}")
                set_clause_str = ', '.join(set_clauses)
                cypher_query = (
                    f"MATCH (n), (m) "
                    f"WHERE n.id = $source_id AND m.id = $target_id "
                    f"MERGE (n)-[r:{edge_type}]->(m) "
                    f"ON CREATE SET {set_clause_str} "
                    f"ON MATCH SET {set_clause_str} "
                    f"RETURN n, r, m"
                )
                params = {"from": source_id, "to": target_id, **edge_data}
                self.neo4j_conn.query(cypher_query, parameters=params)

    def create_relationship_from_node_to_node_by_id(self, tx, from_node_id: str, to_node_id: str, rel_type: str, rel_props: dict):
        """
        Creates a relationship between two nodes in the Neo4j database by type.

        Args:
            tx (Transaction): The Neo4j transaction object.
            from_node_id (str): The ID of the source node.
            to_node_id (str): The ID of the target node.
            rel_type (str): The type of the relationship.
            rel_props (dict): A dictionary containing the relationship properties.

        Returns:
            dict: A dictionary containing the number of nodes created, relationships created, and nodes matched.
            Example:
            {
                'nodes_created': 10,
                'relationships_created': 5,
                'nodes_matched': 3
            }
        """
        try:
            tx.run(
                f"""
                MATCH (a), (b)
                WHERE a.id = $from_node_id AND b.id = $to_node_id
                MERGE (a)-[r:{rel_type}]->(b)
                SET r += $props
                """,
                from_node_id=from_node_id,
                to_node_id=to_node_id,
                props=rel_props,
            )
        except Exception as e:
            print("Execution had an error: ", e)

        result = self.neo4j_conn.query("""
            MATCH (n)
            RETURN count(n) AS nodes_created
        """).single()
        nodes_created = result['nodes_created']

        result = self.neo4j_conn.query("""
            MATCH ()-[r]->()
            RETURN count(r) AS relationships_created
        """).single()
        relationships_created = result['relationships_created']

        result = self.neo4j_conn.query("""
            MATCH (n)
            RETURN count(n) AS nodes_matched
        """).single()
        nodes_matched = result['nodes_matched']

        return {
            'nodes_created': nodes_created,
            'relationships_created': relationships_created,
            'nodes_matched': nodes_matched
        }
    
    def create_relationship_from_node_to_node_by_property(self, tx, from_node_label: str, to_node_label: str, from_property_name: str, to_property_name: str, rel_type: str, rel_props: dict):
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
            dict: A dictionary containing the number of nodes created, relationships created, and nodes matched.
            Example:
            {
                'nodes_created': 10,
                'relationships_created': 5,
                'nodes_matched': 3
            }
        """
        try:
            tx.run(
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

        result = self.neo4j_conn.query("""
            MATCH (n)
            RETURN count(n) AS nodes_created
        """).single()
        nodes_created = result['nodes_created']

        result = self.neo4j_conn.query("""
            MATCH ()-[r]->()
            RETURN count(r) AS relationships_created
        """).single()
        relationships_created = result['relationships_created']

        result = self.neo4j_conn.query("""
            MATCH (n)
            RETURN count(n) AS nodes_matched
        """).single()
        nodes_matched = result['nodes_matched']

        return {
            'nodes_created': nodes_created,
            'relationships_created': relationships_created,
            'nodes_matched': nodes_matched
        }

    def generate_constraints(self, schema):
        for constraint_name, constraint_data in schema['constraints'].items():
            label = constraint_data['label']
            property_name = constraint_data['property']
            cypher_query = f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_name} IS UNIQUE;"
            self.neo4j_conn.query(cypher_query)


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


# Let's test Jay's data schema
def test_neo4j_operations(conn, csv_file_path):
    try:
        # Extract headers from the CSV
        with open(csv_file_path, 'r') as file:
            headers = file.readline().strip().split(',')

        # Add a test property to each header as a node in Neo4j
        conn.add_test_property(headers)

        # Inspect the schema
        schema_info = conn.inspect_schema()
        print(schema_info)

        # Delete the test data
        conn.delete_test_data()
        print("Test data deleted.")
    finally: pass
    #    conn.close()


# Connection to Neo4j
#password="july-bottles-tension"  # "FROM SANDBOX"
#uri="bolt://44.222.238.169:7687"
#user="neo4j"
password="65465dsfg23480dkml2i38lkdsp923"  # "PASSWORD_IS_IN_BITWARDEN"
uri="neo4j://neo4j.neo4j.svc.cluster.local:7687"
user="neo4j"

# Initialize connection
conn = Neo4jConnection(uri, user, password)

databases = conn.show_databases()

for db in databases:
    print(db)


# Adapt CSV to graph using batch
def adapt_csv_to_graph_with_batch(csv_file_path, schema_json_path, output_json_path, batch_size=None):
    # Load schema from JSON file
    with open(schema_json_path) as json_file:
        schema = json.load(json_file)

    nodes = []
    relationships = []
    i = 0  # Initialize a counter for custom ID generation

    # Mapping from CSV column names to node property names
    label_to_properties = {node['labels'][0]: node['properties'] for node in schema['nodes']}

    # Process CSV file
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for j, row in enumerate(reader):
            if batch_size is not None and j >= batch_size:
                break  # Stop processing if batch_size limit is reached

            # For each node label found in label_to_properties, create a node
            for label, properties in label_to_properties.items():
                node_properties = {prop: row[prop] for prop in properties if prop in row}

                # Add 'name' property based on the row's value for the node label if present
                node_properties['name'] = row.get(label, "")

                if node_properties:  # Check if there are any properties to add
                    node = {
                        "id": f"n{i}",  # Customized ID generation
                        "labels": [label],
                        "properties": node_properties
                    }
                    nodes.append(node)
                    i += 1

            # Assuming relationships are defined by the presence of specific columns
            # This needs to be adapted based on a general logic to determine relationships from the schema and CSV
            if 'Genome' in row and 'BGC' in row:
                relationships.append({
                    'from': row['Genome'],
                    'to': row['BGC'],
                    'type': 'CONTAINS'
                })
            if 'Taxonomy' in row and 'BGC' in row:
                relationships.append({
                    'from': row['Taxonomy'],
                    'to': row['BGC'],
                    'type': 'CONTAINS'
                })
            if 'product' in row and 'BGC' in row:
                relationships.append({
                    'from': row['BGC'],
                    'to': row['product'],
                    'type': 'PRODUCES'
                })

    # Create the output model
    output_model = {
        'nodes': nodes,
        'relationships': relationships
    }

    # Write the output model to a JSON file
    with open(output_json_path, 'w') as output_file:
        json.dump(output_model, output_file, indent=4)

    print(f"Adapted model with batch size {'all' if batch_size is None else batch_size} saved to {output_json_path}")


# Push to Neo4J

# Usage
csv_file_path = 'data/Microbiomics_BGC_dataset_test.csv' # Update with actual path
schema_json_path = 'schema.json' # Update with actual path
output_json_path = 'payload.json' # Update with actual path
batch_size = 10 # Set to None to process the whole CSV

adapt_csv_to_graph_with_batch(csv_file_path, schema_json_path, output_json_path)

# Initialize connection
conn = Neo4jConnection(uri, user, password)

gen_push = GraphGenerator(conn)


gen_push.execute_from_json(output_json_path)

conn.inspect_schema()

query_test = "MATCH ()-[r:PRODUCES]->() RETURN count(r) AS total"
#"MATCH (n) RETURN count(n) AS totalNodes"
#"MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN n, r"

conn.query(query_test)
conn.show_databases()
# Close the connection
conn.close()

