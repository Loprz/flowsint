"""
Building Type

Represents a building footprint from Overture Maps.
"""
from typing import Optional
from pydantic import Field
from flowsint_types.flowsint_base import FlowsintType
from flowsint_types.registry import flowsint_type


@flowsint_type
class Building(FlowsintType):
    """A building footprint and associated metadata."""

    gers_id: str = Field(..., description="Global Entity Reference System ID", title="GERS ID", json_schema_extra={"primary": True})
    height: Optional[float] = Field(None, description="Building height in meters")
    levels: Optional[int] = Field(None, description="Number of levels/stories")
    type_class: Optional[str] = Field(None, description="Building class or usage type")
    latitude: float = Field(..., description="Latitude of building centroid")
    longitude: float = Field(..., description="Longitude of building centroid")
    name: Optional[str] = Field(None, description="Descriptive name or label")
    geometry: Optional[str] = Field(None, description="Building footprint as WKT string")

    # Neo4j integration
    @classmethod
    def neo4j_label(cls) -> str:
        return "Building"

    @classmethod
    def neo4j_unique_key(cls) -> str:
        return "gers_id"
