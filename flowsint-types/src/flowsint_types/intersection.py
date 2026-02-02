"""
Intersection Type

Represents a road intersection (connector) in the transportation network.
Used for routing and network analysis.
"""
from typing import Optional
from pydantic import Field
from flowsint_types.flowsint_base import FlowsintType


class Intersection(FlowsintType):
    """A road intersection (connector point in the transportation network)."""

    intersection_id: str = Field(..., description="Unique identifier for the intersection")
    gers_id: Optional[str] = Field(None, description="Global Entity Reference System ID")
    latitude: float = Field(..., description="Latitude coordinate")
    longitude: float = Field(..., description="Longitude coordinate")
    name: Optional[str] = Field(None, description="Intersection name if available")
    source: Optional[str] = Field(None, description="Data source")

    # Neo4j integration
    @classmethod
    def neo4j_label(cls) -> str:
        return "Intersection"

    @classmethod
    def neo4j_unique_key(cls) -> str:
        return "intersection_id"
