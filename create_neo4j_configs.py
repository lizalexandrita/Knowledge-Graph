import json

config = {
    "uri": "bolt://localhost:7687",
    "user": "neo4j",
    "password": "neo4j@test_db"
}

with open("config_neo4j.json", "w") as file:
    json.dump(config, file)