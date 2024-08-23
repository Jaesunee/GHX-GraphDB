from neo4j import GraphDatabase

class Neo4jExporter:
    def __init__(self, uri, user, password, database=None):
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password), database=self._database)
    
    def export_to_graphml(self, output_file):
        with self._driver.session() as session:
            result = session.run("""
                CALL apoc.export.graphml.all($outputFile, {})
                YIELD nodes, relationships, properties, time
                RETURN nodes, relationships, properties, time
            """, outputFile=output_file)
            record = result.single()
            print("Exported {} nodes and {} relationships to {}".format(record["nodes"], record["relationships"], output_file))
    
    def close(self):
        self._driver.close()

if __name__ == "__main__":
    uri = "bolt://10.32.32.90:7687"  # Update with your Neo4j URI
    user = "neo4j"                    # Update with your Neo4j username
    password = "password"             # Update with your Neo4j password
    database = "neo4j"                # Update with your Neo4j database name
    output_file = "/home/ubuntu/GHX-Capstone-Code/code/neo4j test/graph.graphml"     # Specify the output file name

    exporter = Neo4jExporter(uri, user, password, database)
    exporter.export_to_graphml(output_file)
    exporter.close()