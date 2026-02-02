from typing import List, Optional, Self
from pydantic import Field, model_validator
from .flowsint_base import FlowsintType
from .registry import flowsint_type


@flowsint_type
class Place(FlowsintType):
    """Represents a place/POI from Overture Maps or similar geospatial data sources."""

    name: str = Field(
        ...,
        description="Name of the place",
        title="Place Name",
        json_schema_extra={"primary": True},
    )
    gers_id: Optional[str] = Field(
        None,
        description="Global Entity Reference System ID",
        title="GERS ID",
    )
    category: str = Field(
        ...,
        description="Primary category of the place (e.g., restaurant, bank, hotel)",
        title="Category",
    )
    latitude: float = Field(
        ...,
        description="Latitude coordinate",
        title="Latitude",
        ge=-90,
        le=90,
    )
    longitude: float = Field(
        ...,
        description="Longitude coordinate",
        title="Longitude",
        ge=-180,
        le=180,
    )
    address: Optional[str] = Field(
        None,
        description="Full street address of the place",
        title="Address",
    )
    city: Optional[str] = Field(
        None,
        description="City where the place is located",
        title="City",
    )
    country: Optional[str] = Field(
        None,
        description="Country where the place is located",
        title="Country",
    )
    confidence: Optional[float] = Field(
        None,
        description="Confidence score from the data source (0-1)",
        title="Confidence",
        ge=0,
        le=1,
    )
    source: Optional[str] = Field(
        None,
        description="Data source (e.g., meta, msft, overture)",
        title="Source",
    )
    overture_id: Optional[str] = Field(
        None,
        description="Unique identifier from Overture Maps",
        title="Overture ID",
    )
    brand: Optional[str] = Field(
        None,
        description="Brand name if applicable",
        title="Brand",
    )
    websites: Optional[List[str]] = Field(
        None,
        description="Associated website URLs",
        title="Websites",
    )
    phones: Optional[List[str]] = Field(
        None,
        description="Associated phone numbers",
        title="Phone Numbers",
    )
    socials: Optional[List[str]] = Field(
        None,
        description="Social media profile URLs",
        title="Social Profiles",
    )
    hours: Optional[str] = Field(
        None,
        description="Operating hours (human-readable format)",
        title="Hours",
    )

    @model_validator(mode="after")
    def compute_label(self) -> Self:
        """Generate the node label from name and category."""
        if self.city:
            self.nodeLabel = f"{self.name} ({self.category}) - {self.city}"
        else:
            self.nodeLabel = f"{self.name} ({self.category})"
        return self

    @classmethod
    def detect(cls, line: str) -> bool:
        """Place cannot be reliably detected from a single line of text."""
        return False
