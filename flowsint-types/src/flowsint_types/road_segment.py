"""
Road Segment Type

Represents a road segment from transportation network data.
Used for routing and network analysis.
"""
from typing import Any, List, Optional
from pydantic import Field
from flowsint_types.flowsint_base import FlowsintType


class RoadSegment(FlowsintType):
    """A road segment connecting two intersections."""

    segment_id: str = Field(..., description="Unique identifier for the segment")
    gers_id: Optional[str] = Field(None, description="Global Entity Reference System ID")
    name: Optional[str] = Field(None, description="Road name")
    road_class: str = Field(
        default="unknown",
        description="Road classification (primary, secondary, tertiary, etc.)",
    )
    length_m: float = Field(default=0.0, description="Segment length in meters")
    start_connector_id: str = Field(..., description="ID of the starting intersection")
    end_connector_id: str = Field(..., description="ID of the ending intersection")
    oneway: bool = Field(default=False, description="Whether the road is one-way")
    max_speed_kmh: Optional[int] = Field(None, description="Speed limit in km/h")
    coordinates: Optional[List[List[float]]] = Field(
        None, description="Polyline coordinates as [[lat, lon], ...]"
    )
    source: Optional[str] = Field(None, description="Data source")

    # Neo4j integration
    @classmethod
    def neo4j_relationship_type(cls) -> str:
        return "ROAD_SEGMENT"

    @classmethod
    def neo4j_start_label(cls) -> str:
        return "Intersection"

    @classmethod
    def neo4j_end_label(cls) -> str:
        return "Intersection"
