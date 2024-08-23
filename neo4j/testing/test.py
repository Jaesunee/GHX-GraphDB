from neo4j import GraphDatabase
import networkx as nx

# Read graphML file
graph = nx.read_graphml("./graph.graphml")

# file_path = "./graph.graphml"

# Establish connection to Neo4j

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"), database="neo4j")
session = driver.session()


# Create nodes and relationships in Neo4j
for node, data in graph.nodes(data=True):
    session.run(f"CREATE (n:{data['labelV']}) SET n = $node_props", node_props=data)
    print(node)
    print(data)



session.close()