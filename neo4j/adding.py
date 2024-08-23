import neo4j as neo
from neo4j import GraphDatabase
import pandas as pd
import sys
import decimal
# append a new directory to sys.path
# TODO: change based on your file structure
sys.path.append('../data_pull_scripts')
from pull_data import return_dfs

# EXAMPLE COMMAND TO START SERVER: sudo /opt/neo4j-community-5.17.0/bin/neo4j console

result = []
result = return_dfs()
node_dfs = result[0]
edge_dfs = result[1]
node_names = result[2]
edge_names = result[3]

driver = GraphDatabase.driver("bolt://10.32.32.90:7687", auth=("neo4j", "password"), database="neo4j")
session = driver.session()

# Delete existing graph
delete_query = "MATCH (n) DETACH DELETE n"
session.run(delete_query)

# add nodes based on pulled data frames containing node data (sql data from snowflake)
for i, df in enumerate(node_dfs):
    name = node_names[i]
    print(name)
    make_node_query = f"CREATE (n:{name}) SET n = $node_props"
    
    for index, row in df.iterrows():
        props = dict(row)
        
        # Convert decimal.Decimal values to float or str if needed
        for key, value in props.items():
            if isinstance(value, decimal.Decimal):
                props[key] = float(value)  # Convert to float
                # Or: props[key] = str(value)  # Convert to string
        
        session.run(make_node_query, node_props=props)
        
iteration_count = 0
for i, df in enumerate(edge_dfs):
    name = edge_names[i]
    for index, row in df.iterrows():
        iteration_count += 1
        # Extract the column names from the DataFrame
        source_col = df.columns[0]
        target_col = df.columns[1]
        
        # Extract the properties for the relationship from the row
        source_value = row[source_col]
        target_value = row[target_col]
        
        if target_col == 'primary_eid':
            target_value = f"'{target_value}'"
        
        create_relationship_query = f"""
        MATCH (a) , (b)
        WHERE a.{source_col} = {source_value} AND b.{target_col} = {target_value}
        CREATE (a)-[:{name}]->(b)
        """
    
        print(f"Number of iterations: {iteration_count}, Cypher Query: {create_relationship_query}")
        
        # Run the query inside a try-except block
        try:
            session.run(create_relationship_query)
        except Exception as e:
            print(f"Error occurred while executing query: {e}")
            continue  # Skip to the next iteration if an error occurs

session.close()