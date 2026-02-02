
from neo4j import GraphDatabase
import os

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def check_gers():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    query_gers = """
    MATCH (n) 
    WHERE n.gers_id = '22c50548-a22f-4f56-a02b-292fdadc9ac2'
    RETURN n.nodeLabel, labels(n), n.gers_id
    """
    
    query_same_as = """
    MATCH (n)-[r:SAME_AS]-(m) 
    WHERE n.address CONTAINS '13761' OR m.address CONTAINS '13761'
    RETURN n.nodeLabel, type(r), m.nodeLabel, m.gers_id
    """

    with driver.session() as session:
        print("--- Checking for GERS ID Node ---")
        result = session.run(query_gers)
        found = False
        for record in result:
            found = True
            print(f"Found Node: {record['n.nodeLabel']} Labels: {record['labels(n)']}")
        if not found:
            print("Node with GERS ID NOT FOUND in Neo4j.")

        print("\n--- Checking for SAME_AS Relationships ---")
        result = session.run(query_same_as)
        found_edge = False
        for record in result:
            found_edge = True
            print(f"Found Edge: {record['n.nodeLabel']} --[{record['type(r)']}]--> {record['m.nodeLabel']} (GERS: {record['m.gers_id']})")
        if not found_edge:
            print("No SAME_AS edges found for this address.")

    driver.close()

if __name__ == "__main__":
    check_gers()
