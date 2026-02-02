"""
Administrative Division Type

Represents an administrative boundary (e.g., City, County, Region, Country).
"""
from typing import Optional
from pydantic import Field
from flowsint_types.flowsint_base import FlowsintType
from flowsint_types.registry import flowsint_type


@flowsint_type
class Division(FlowsintType):
    """An administrative division boundary."""

    gers_id: str = Field(..., description="Global Entity Reference System ID", title="GERS ID", json_schema_extra={"primary": True})
    division_id: Optional[str] = Field(None, description="Source-specific division ID")
    name: str = Field(..., description="Name of the division (e.g. 'New York')")
    subtype: str = Field(..., description="Type of division (locality, region, country, etc.)")
    country_iso: Optional[str] = Field(None, description="ISO 3166-1 alpha-2 country code")
    
    # Neo4j integration
    @classmethod
    def neo4j_label(cls) -> str:
        return "Division"

    @classmethod
    def neo4j_unique_key(cls) -> str:
        return "gers_id"
