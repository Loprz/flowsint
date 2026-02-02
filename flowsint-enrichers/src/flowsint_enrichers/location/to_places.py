"""
Location to Places Enricher
"""
from __future__ import annotations
import sys
from typing import Any, List, Optional, Type
from pydantic import BaseModel, Field

from flowsint_core.core.enricher_base import Enricher
from flowsint_core.core.logger import Logger
from flowsint_enrichers.registry import flowsint_enricher
from flowsint_types import Location, Place

# Import Overture client - handle import error gracefully
try:
    from tools.overture.client import get_overture_client
except ImportError:
    # Fallback for when tools is not in the path
    import sys
    import os
    tools_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "tools")
    sys.path.insert(0, tools_path)
    from overture.client import get_overture_client


class LocationToPlacesParams(BaseModel):
    """Parameters for the LocationToPlaces enricher."""

    radius_km: float = Field(
        default=1.0,
        description="Search radius in kilometers",
        ge=0.1,
        le=50.0,
    )
    categories: Optional[list[str]] = Field(
        default=None,
        description="Filter by place categories (e.g., restaurant, hotel, bank)",
    )
    limit: int = Field(
        default=25,
        description="Maximum number of places to return",
        ge=1,
        le=100,
    )


@flowsint_enricher
class LocationToPlacesEnricher(Enricher):
    """[Overture Maps] Find nearby places for a location using Overture Maps data."""

    InputType = Location
    OutputType = Place

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
                "name": "radius_km",
                "type": "number",
                "description": "Search radius in kilometers",
                "required": False,
                "default": 5.0,
            },
            {
                "name": "categories",
                "type": "string",
                "description": "Filter by place categories (comma-separated, e.g., 'restaurant, hotel')",
                "required": False,
            },
            {
                "name": "limit",
                "type": "number",
                "description": "Maximum number of places to return",
                "required": False,
                "default": 25,
            },
        ]

    @classmethod
    def name(cls) -> str:
        return "location_to_places"

    @classmethod
    def category(cls) -> str:
        return "Location"

    @classmethod
    def key(cls) -> str:
        return "address"

    @classmethod
    def documentation(cls) -> str:
        return """
        This enricher queries Overture Maps to find places (POIs) near a given location.
        
        It requires the location to have latitude and longitude coordinates.
        You can filter results by category (e.g., restaurant, hotel, bank) and
        adjust the search radius.
        
        The enricher returns Place entities with name, category, coordinates,
        and contact information when available.
        """

    async def scan(self, data: list[InputType]) -> list[OutputType]:
        """Query Overture Maps for places near each location."""
        Logger.info(self.sketch_id, {"message": f"Overture scan started for {len(data)} locations"})
        results: list[OutputType] = []
        client = get_overture_client()

        # Get parameters
        radius_km = float(self.params.get("radius_km", 5.0))
        limit = int(self.params.get("limit", 25))
        categories_raw = self.params.get("categories")
        
        # Treatment of "all" or empty as no filter
        if categories_raw and categories_raw.lower().strip() != "all":
            categories_list = [c.strip() for c in categories_raw.split(",")]
        else:
            categories_list = None

        Logger.info(self.sketch_id, {"message": f"Parameters: radius={radius_km}km, limit={limit}, categories={categories_raw or 'all'}"})

        for location in data:
            Logger.info(self.sketch_id, {"message": f"Raw location data: {location.model_dump_json()}"})
            Logger.info(self.sketch_id, {"message": f"Checking location: {location.address} (lat={location.latitude}, lon={location.longitude})"})
            # Check if location has coordinates
            if location.latitude is None or location.longitude is None:
                Logger.info(self.sketch_id, {"message": f"Skipping location {location.address} - missing coordinates"})
                Logger.info(
                    self.sketch_id,
                    {"message": f"Location '{location.address}' missing coordinates, skipping"},
                )
                continue

            try:
                Logger.info(self.sketch_id, {"message": f"Querying Overture near {location.latitude}, {location.longitude}"})
                # Query Overture Maps
                places_data = client.query_places_near_point(
                    latitude=location.latitude,
                    longitude=location.longitude,
                    radius_km=radius_km,
                    categories=categories_list,
                    limit=limit,
                )
                Logger.info(self.sketch_id, {"message": f"Overture returned {len(places_data)} results"})

                # Convert to Place objects
                for place_dict in places_data:
                    try:
                        place = Place(
                            name=place_dict["name"],
                            category=place_dict["category"],
                            latitude=place_dict["latitude"],
                            longitude=place_dict["longitude"],
                            address=place_dict.get("address"),
                            confidence=place_dict.get("confidence"),
                            source=place_dict.get("source"),
                            overture_id=place_dict.get("overture_id"),
                            brand=place_dict.get("brand"),
                            websites=place_dict.get("websites"),
                            phones=place_dict.get("phones"),
                            socials=place_dict.get("socials"),
                        )
                        results.append(place)
                    except Exception as e:
                        Logger.debug(
                            self.sketch_id,
                            {"message": f"Error creating Place: {e}"},
                        )
                        continue

                Logger.info(
                    self.sketch_id,
                    {
                        "message": f"Found {len(places_data)} places near '{location.address}'"
                    },
                )

            except Exception as e:
                Logger.error(
                    self.sketch_id,
                    {"message": f"Error querying Overture Maps: {e}"},
                )
                continue

        return results

    def postprocess(
        self, results: list[OutputType], original_input: list[InputType]
    ) -> list[OutputType]:
        """Create graph nodes and relationships."""
        if not self._graph_service:
            return results

        # Create Location nodes
        for location in original_input:
            if location.latitude is not None and location.longitude is not None:
                self.create_node(location)

        # Create Place nodes and relationships
        for place in results:
            self.create_node(place)

            # Link to the nearest original location
            nearest_location = self._find_nearest_location(place, original_input)
            if nearest_location:
                self.create_relationship(nearest_location, place, "HAS_NEARBY_PLACE")

            self.log_graph_message(
                f"Place found: {place.name} ({place.category}) at ({place.latitude:.4f}, {place.longitude:.4f})"
            )

        return results

    def _find_nearest_location(
        self, place: Place, locations: list[Location]
    ) -> Optional[Location]:
        """Find the nearest location to a place."""
        import math

        min_distance = float("inf")
        nearest = None

        for location in locations:
            if location.latitude is None or location.longitude is None:
                continue

            # Simple Euclidean distance (good enough for nearby points)
            distance = math.sqrt(
                (place.latitude - location.latitude) ** 2
                + (place.longitude - location.longitude) ** 2
            )

            if distance < min_distance:
                min_distance = distance
                nearest = location

        return nearest


# Export types
InputType = LocationToPlacesEnricher.InputType
OutputType = LocationToPlacesEnricher.OutputType
