import json

def create_neo4j_configs():
    """
    Creates a Neo4j configuration file with the specified URI, username, and password.

    Returns:
        None

    Raises:
        IOError: If there is an error writing the configuration file.

    Example Usage:
        create_neo4j_configs()
    """
    config = {
        "uri": "bolt://localhost:7687",
        "user": "neo4j",
        "password": "neo4j@test_db"
    }

    try:
        with open("config_neo4j.json", "w") as file:
            json.dump(config, file)
    except IOError as e:
        print(f"Error writing configuration file: {e}")

# Call the function to create the Neo4j configuration file
create_neo4j_configs()
