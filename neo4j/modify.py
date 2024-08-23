import neo4j as neo
from neo4j import GraphDatabase
import pandas as pd
import sys
import decimal
# append a new directory to sys.path
sys.path.append('/home/ubuntu/data_pull_scripts')
from pull_data import return_dfs


# COMMAND TO START SERVER: sudo /opt/neo4j-community-5.17.0/bin/neo4j console

result = []
result = return_dfs()
node_dfs = result[0]
edge_dfs = result[1]
node_names = result[2]
edge_names = result[3]
edge_labels = result[4]

driver = GraphDatabase.driver("bolt://10.32.32.90:7687", auth=("neo4j", "password"), database="neo4j")
session = driver.session()

# run a query on graph
# TODO: create relationship (i.e. trades with relationship) for nodes that match the following query:
# MATCH (p:party)-->(s:sup_item)<--(t:transaction)-->(pi:party_instance) return p, s, t, pi
query = ""
session.run(query)

session.close()