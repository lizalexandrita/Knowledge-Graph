import csv, json
from kg_nal import Neo4jConnection, GraphGenerator, ParseData
# Connection to Neo4j
#password="july-bottles-tension"  # "FROM SANDBOX"
#uri="bolt://44.222.238.169:7687"
#user="neo4j"
password= "neo4j@test_db"  # "65465dsfg23480dkml2i38lkdsp923"  # "PASSWORD_IS_IN_BITWARDEN"
uri= "bolt://localhost:7687"  # "neo4j://neo4j.neo4j.svc.cluster.local:7687"
user="neo4j"

# Initialize connection
conn = Neo4jConnection(uri, user, password)

# databases = conn.show_databases()

# for db in databases:
#     print(db)


# Adapt CSV to graph using batch
""" 
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
"""
# Push to Neo4J

""" 

# Usage
csv_file_path = 'data/Microbiomics_BGC_dataset_test.csv' # Update with actual path
schema_json_path = 'schema.json' # Update with actual path
output_json_path = 'payload.json' # Update with actual path
batch_size = 10 # Set to None to process the whole CSV

adapt_csv_to_graph_with_batch(csv_file_path, schema_json_path, output_json_path)

# Initialize connection
# conn = Neo4jConnection(uri, user, password)

gen_push = GraphGenerator(conn)


gen_push.execute_from_json(output_json_path)

conn.inspect_schema()

# query_test = "MATCH ()-[r:PRODUCES]->() RETURN count(r) AS total"
#"MATCH (n) RETURN count(n) AS totalNodes"
#"MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN n, r"
conn.query(query_test)
conn.show_databases()



# Extract data from json files Semidan
compounds_json = "src_semi/modelSEED/compounds.json"
reactions_json = "src_semi/modelSEED/reactions.json"

# Extract node from json files
compounds = ParseData.extract_node_from_json(compounds_json)
reactions = ParseData.extract_node_from_json(reactions_json)

# Create nodes from dict
genn = GraphGenerator(conn)

node_compound = [genn.merge_node_from_dict("Compound", compound) for compound in compounds]
node_reaction = [genn.merge_node_from_dict("Reaction", reaction) for reaction in reactions]
"""


genn = GraphGenerator(conn)

"""
# Create relationships between reaction node and compound nodes
# Access the database and parse all the reactions
reactions = conn.query("MATCH (r:Reaction) RETURN r.compound_ids AS compound_ids, r.id AS reaction_id")

# Create relationships between reaction node and compound nodes
for reaction in reactions:
    compound_ids = reaction['compound_ids'].split(';')
    reaction_id = reaction['reaction_id']
    for compound_id in compound_ids:
        rel_props = {
            'type': 'PARTICIPATES_IN'
        }
        genn.merge_relationship_from_node_to_node_by_id(compound_id, reaction_id, 'PARTICIPATES_IN', rel_props)
"""

"""
# Create relationships between reaction nodes
# Access the database and parse all the reactions
reactions = conn.query("MATCH (r:Reaction) RETURN r.id AS reaction_id, r.linked_reaction AS linked_reaction")

# Create relationships between reaction nodes
for reaction in reactions:
    try:
        linked_reactions = reaction.get('linked_reaction', '').split(';')
        reaction_id = reaction['reaction_id']
        for linked_reaction in linked_reactions:
            rel_props = {
                'type': 'LINKED_TO'
            }
            genn.merge_relationship_from_node_to_node_by_id(reaction_id, linked_reaction, 'LINKED_TO', rel_props)
    except Exception as e:
        pass  # print("Execution Exception: ", e)  
"""



"""
# Parse the reaction equation and create relationships between compound nodes
# Access the database and parse all the reactions
reactions = conn.query("MATCH (r:Reaction) RETURN r.id AS reaction_id, r.equation AS equation")

# Create relationships between compound and reaction nodes
for reaction in reactions:
    try:
        substrates, products = ParseData.parse_reaction_equation(reaction['equation'])
        reaction_id = reaction['reaction_id']
        for substrate, stoichiometry in substrates:
            genn.merge_relationship_from_node_to_node_by_id(substrate, reaction_id, 'SUBSTRATE_OF', {"stoichiometry": stoichiometry})
        for product, stoichiometry in products:
            genn.merge_relationship_from_node_to_node_by_id(product, reaction_id, 'PRODUCT_OF', {"stoichiometry": stoichiometry})
    except Exception as e:
        pass  # print("Execution Exception: ", e)
    """



# Close the connection
# conn.close()

