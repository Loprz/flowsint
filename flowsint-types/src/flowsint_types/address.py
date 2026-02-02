from pydantic import Field, model_validator
from typing import Optional, Self
from .flowsint_base import FlowsintType
from .registry import flowsint_type


@flowsint_type
class Location(FlowsintType):
    """Represents a physical address with geographical coordinates."""

    address: str = Field(..., description="Street address", title="Street Address", json_schema_extra={"primary": True})
    gers_id: Optional[str] = Field(
        None,
        description="Global Entity Reference System ID",
        title="GERS ID",
    )
    city: Optional[str] = Field(None, description="City name", title="City")
    country: Optional[str] = Field(None, description="Country name", title="Country")
    zip: Optional[str] = Field(None, description="ZIP or postal code", title="ZIP/Postal Code")
    latitude: Optional[float] = Field(
        None, description="Latitude coordinate of the address", title="Latitude"
    )
    longitude: Optional[float] = Field(
        None, description="Longitude coordinate of the address", title="Longitude"
    )

    @model_validator(mode="after")
    def compute_label(self) -> Self:
        if not self.nodeLabel:
            self.nodeLabel = self.address
        return self

    @classmethod
    def detect(cls, line: str) -> bool:
        """Location cannot be reliably detected from a single line of text."""
        return False
