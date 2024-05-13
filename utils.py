from kg_nal import Neo4jConnection

PASSWORD="691723fg3480dkml2i38lkdsp923"

# Connection to Neo4j
conn = Neo4jConnection(uri="neo4j+s://n4j.newatlantis.dev:7687", user="neo4j", pwd=PASSWORD)

databases = conn.show_databases()

for db in databases:
    print(db)

conn.close()