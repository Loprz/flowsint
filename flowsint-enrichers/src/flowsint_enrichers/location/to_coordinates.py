"""
Location to Coordinates Enricher
"""
from __future__ import annotations
import requests
import sys
from typing import Any, List, Optional, Type
from pydantic import BaseModel, Field

from flowsint_core.core.enricher_base import Enricher
from flowsint_core.core.logger import Logger
from flowsint_enrichers.registry import flowsint_enricher
from flowsint_types import Location


class LocationToCoordinatesParams(BaseModel):
    """Parameters for the LocationToCoordinates enricher."""

    limit: int = Field(
        default=1,
        description="Maximum number of coordinate matches to return per address",
        ge=1,
        le=5,
    )


@flowsint_enricher
class LocationToCoordinatesEnricher(Enricher):
    """[Nominatim] Convert physical addresses into GPS coordinates."""

    InputType = Location
    OutputType = Location

    def __init__(
        self,
        sketch_id: Optional[str] = None,
        scan_id: Optional[str] = None,
        vault=None,
        params: Optional[dict[str, Any]] = None,
    ):
        super().__init__(
            sketch_id=sketch_id,
            scan_id=scan_id,
            params_schema=self.get_params_schema(),
            vault=vault,
            params=params,
        )

    @classmethod
    def get_params_schema(cls) -> list[dict[str, Any]]:
        """Declare parameters for this enricher"""
        return [
            {
                "name": "limit",
                "type": "number",
                "description": "Maximum number of coordinate matches to return per address",
                "required": False,
                "default": 1,
            }
        ]

    @classmethod
    def name(cls) -> str:
        return "location_to_coordinates"

    @classmethod
    def category(cls) -> str:
        return "Location"

    @classmethod
    def key(cls) -> str:
        return "address"

    @classmethod
    def documentation(cls) -> str:
        return """
        This enricher geocodes street addresses into latitude and longitude coordinates
        using the OpenStreetMap Nominatim service.
        
        It is essential for enabling other geospatial enrichers (like Overture Maps)
        which require coordinates to function.
        
        Note: Please respect the Nominatim Usage Policy (max 1 request per second).
        """

    async def scan(self, data: list[InputType]) -> list[OutputType]:
        """Geocode each location address."""
        Logger.info(self.sketch_id, {"message": f"Geocoding {len(data)} locations"})
        results: list[OutputType] = []
        
        # Get parameters
        limit = int(self.params.get("limit", 1))

        for location in data:
            Logger.info(self.sketch_id, {"message": f"Processing location: {location.address}"})
            try:
                # Build query string
                query = location.address
                if location.city:
                    query += f", {location.city}"
                if location.country:
                    query += f", {location.country}"
                
                Logger.info(self.sketch_id, {"message": f"Query string: {query}"})

                # Query Nominatim
                # User-Agent is required by Nominatim policy
                headers = {
                    "User-Agent": "Flowsint/1.0 (OSINT Investigation Tool)"
                }
                url = "https://nominatim.openstreetmap.org/search"
                params_api = {
                    "q": query,
                    "format": "json",
                    "limit": limit
                }

                Logger.debug(
                    self.sketch_id,
                    {"message": f"Geocoding address: {query}"}
                )

                response = requests.get(url, params=params_api, headers=headers, timeout=10)
                response.raise_for_status()
                matches = response.json()

                if not matches:
                    Logger.info(self.sketch_id, {"message": f"No coordinates found for address: {query}"})
                    continue

                Logger.info(self.sketch_id, {"message": f"Found {len(matches)} matches"})

                for match in matches:
                    try:
                        # Update existing location coordinates (for the first match mostly)
                        # or create new location variants
                        lat = float(match.get("lat"))
                        lon = float(match.get("lon"))
                        
                        # Create a copy or update original
                        enriched_location = Location(
                            address=location.address,
                            city=location.city or match.get("address", {}).get("city", ""),
                            country=location.country or match.get("address", {}).get("country", ""),
                            zip=location.zip or match.get("address", {}).get("postcode", ""),
                            latitude=float(match["lat"]),
                            longitude=float(match["lon"]),
                            nodeLabel=location.nodeLabel,
                        )
                        results.append(enriched_location)
                    except (ValueError, TypeError):
                        continue

                Logger.info(
                    self.sketch_id,
                    {"message": f"Successfully geocoded '{location.address}' to ({results[0].latitude}, {results[0].longitude})"}
                )

            except Exception as e:
                Logger.error(
                    self.sketch_id,
                    {"message": f"Geocoding failed for {location.address}: {e}"}
                )
                continue

        return results

    def postprocess(
        self, results: list[OutputType], original_input: list[InputType]
    ) -> list[OutputType]:
        """Update the graph with geocoded coordinates."""
        if not self._graph_service:
            return results

        for location in results:
            if location.latitude is not None and location.longitude is not None:
                self.create_node(location)
                self.log_graph_message(
                    f"Geocoded: {location.address} -> ({location.latitude}, {location.longitude})"
                )

        return results


# Export types
InputType = LocationToCoordinatesEnricher.InputType
OutputType = LocationToCoordinatesEnricher.OutputType
