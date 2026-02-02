import sys
import os
import uuid
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add src directories to path
sys.path.append(os.path.join(os.getcwd(), 'flowsint-core/src'))
sys.path.append(os.path.join(os.getcwd(), 'flowsint-types/src'))
sys.path.append(os.path.join(os.getcwd(), 'flowsint-enrichers/src'))

from flowsint_core.core.models import Flow
from flowsint_core.core.postgre_db import Base

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://flowsint:flowsint@localhost:5433/flowsint")

def seed_flow():
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    flow_name = "Overture Context Harmonization"
    existing_flow = session.query(Flow).filter_by(name=flow_name).first()
    
    if existing_flow:
        print(f"Flow '{flow_name}' already exists. Updating...")
        flow = existing_flow
    else:
        print(f"Creating new Flow '{flow_name}'...")
        flow = Flow(id=uuid.uuid4(), name=flow_name)

    flow.description = "Enrich locations with building footprints and administrative divisions using Overture Maps (GERS)."
    flow.category = ["Context", "Location", "Place"]
    
    # Define Flow Schema
    # 1 Input Node -> 2 Parallel Enricher Nodes
    
    input_node_id = str(uuid.uuid4())
    building_node_id = str(uuid.uuid4())
    division_node_id = str(uuid.uuid4())

    flow_schema = {
        "nodes": [
            {
                "id": input_node_id,
                "position": {"x": 100, "y": 100},
                "data": {
                    "label": "Location Input",
                    "type": "type",
                    "class_name": "Location", # or Place effectively
                    "outputs": {
                        "properties": [{"name": "input_object", "type": "object"}]
                    }
                },
                "type": "custom"
            },
            {
                "id": building_node_id,
                "position": {"x": 300, "y": 50},
                "data": {
                    "label": "Resolve Building",
                    "class_name": "ResolveBuilding", # Matches enricher class name
                    "params": {},
                    "inputs": {"input": "input_object"}, # Maps enricher input 'input' to prev node output
                    "outputs": {
                        "properties": [{"name": "building_info", "type": "object"}]
                    }
                },
                "type": "custom"
            },
            {
                "id": division_node_id,
                "position": {"x": 300, "y": 200},
                "data": {
                    "label": "Resolve Division",
                    "class_name": "ResolveDivision",
                    "params": {},
                    "inputs": {"input": "input_object"},
                    "outputs": {
                        "properties": [{"name": "division_info", "type": "object"}]
                    }
                },
                "type": "custom"
            }
        ],
        "edges": [
            {
                "id": f"e{input_node_id}-{building_node_id}",
                "source": input_node_id,
                "target": building_node_id,
                "sourceHandle": "input_object",
                "targetHandle": "input"
            },
            {
                "id": f"e{input_node_id}-{division_node_id}",
                "source": input_node_id,
                "target": division_node_id,
                "sourceHandle": "input_object",
                "targetHandle": "input"
            }
        ]
    }

    flow.flow_schema = flow_schema
    flow.last_updated_at = datetime.utcnow()

    if not existing_flow:
        session.add(flow)
    
    try:
        session.commit()
        print(f"Successfully saved flow '{flow_name}' with ID {flow.id}")
    except Exception as e:
        session.rollback()
        print(f"Error saving flow: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    seed_flow()
