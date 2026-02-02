import sys
import os
import uuid
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add src directories to path
# Using absolute paths for Docker container environment
sys.path.append('/app/flowsint-core/src')
sys.path.append('/app/flowsint-types/src')
sys.path.append('/app/flowsint-enrichers/src')

from flowsint_core.core.models import Flow
from flowsint_core.core.postgre_db import Base

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://flowsint:flowsint@localhost:5433/flowsint")

def seed_flow():
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception as e:
        print(f"Failed to connect to DB at {DATABASE_URL}: {e}")
        return

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
    building_node_id = f"resolve_building-{uuid.uuid4()}"
    division_node_id = f"resolve_division-{uuid.uuid4()}"
    street_node_id = f"link_address_to_street-{uuid.uuid4()}"
    place_addr_node_id = f"link_place_to_address-{uuid.uuid4()}"
    gers_addr_node_id = f"link_location_to_overture_address-{uuid.uuid4()}"

    flow_schema = {
        "nodes": [
            {
                "id": input_node_id,
                "position": {"x": 100, "y": 250},
                "data": {
                    "label": "Location/Place Input",
                    "type": "type",
                    "class_name": "Location",
                    "outputs": {
                        "properties": [{"name": "input_object", "type": "object"}]
                    }
                },
                "type": "custom"
            },
            {
                "id": building_node_id,
                "position": {"x": 400, "y": 50},
                "data": {
                    "label": "Resolve Building",
                    "class_name": "ResolveBuilding",
                    "params": {},
                    "inputs": {"input": "input_object"},
                    "outputs": {
                        "properties": [{"name": "building_info", "type": "object"}]
                    }
                },
                "type": "custom"
            },
            {
                "id": division_node_id,
                "position": {"x": 400, "y": 200},
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
            },
            {
                "id": street_node_id,
                "position": {"x": 400, "y": 350},
                "data": {
                    "label": "Link to Street",
                    "class_name": "LinkAddressToStreet",
                    "params": {},
                    "inputs": {"input": "input_object"},
                    "outputs": {
                        "properties": [{"name": "street_info", "type": "object"}]
                    }
                },
                "type": "custom"
            },
            {
                "id": place_addr_node_id,
                "position": {"x": 400, "y": 500},
                "data": {
                    "label": "Link Place to Address",
                    "class_name": "LinkPlaceToAddress",
                    "params": {},
                    "inputs": {"input": "input_object"},
                    "outputs": {
                        "properties": [{"name": "address_link_info", "type": "object"}]
                    }
                },
                "type": "custom"
            },
            {
                "id": gers_addr_node_id,
                "position": {"x": 400, "y": 650},
                "data": {
                    "label": "GERS Address Link",
                    "class_name": "LinkLocationToOvertureAddress",
                    "params": {},
                    "inputs": {"input": "input_object"},
                    "outputs": {
                        "properties": [{"name": "address_gers_info", "type": "object"}]
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
            },
            {
                "id": f"e{input_node_id}-{street_node_id}",
                "source": input_node_id,
                "target": street_node_id,
                "sourceHandle": "input_object",
                "targetHandle": "input"
            },
            {
                "id": f"e{input_node_id}-{place_addr_node_id}",
                "source": input_node_id,
                "target": place_addr_node_id,
                "sourceHandle": "input_object",
                "targetHandle": "input"
            },
            {
                "id": f"e{input_node_id}-{gers_addr_node_id}",
                "source": input_node_id,
                "target": gers_addr_node_id,
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
