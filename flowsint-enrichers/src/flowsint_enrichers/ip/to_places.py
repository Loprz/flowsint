"""
IP to Places Enricher
"""
from __future__ import annotations
from typing import Any, List, Optional, Type
from pydantic import BaseModel, Field

from flowsint_core.core.enricher_base import Enricher
from flowsint_core.core.logger import Logger
from flowsint_enrichers.registry import flowsint_enricher
from flowsint_types import Ip, Place

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


class IpToPlacesParams(BaseModel):
    """Parameters for the IpToPlaces enricher."""

    radius_km: float = Field(
        default=5.0,
        description="Search radius in kilometers (larger default for IP geolocation which is less precise)",
        ge=0.5,
        le=100.0,
    )
    categories: Optional[list[str]] = Field(
        default=None,
        description="Filter by place categories (e.g., restaurant, hotel, bank, datacenter)",
    )
    limit: int = Field(
        default=20,
        description="Maximum number of places to return",
        ge=1,
        le=100,
    )


@flowsint_enricher
class IpToPlacesEnricher(Enricher):
    """[Overture Maps] Find nearby places for an IP address using its geolocation."""

    InputType = Ip
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
                "description": "Filter by place categories (comma-separated, e.g., 'datacenter, office')",
                "required": False,
            },
            {
                "name": "limit",
                "type": "number",
                "description": "Maximum number of places to return",
                "required": False,
                "default": 20,
            },
        ]

    @classmethod
    def name(cls) -> str:
        return "ip_to_places"

    @classmethod
    def category(cls) -> str:
        return "Ip"

    @classmethod
    def key(cls) -> str:
        return "address"

    @classmethod
    def documentation(cls) -> str:
        return """
        This enricher queries Overture Maps to find places (POIs) near an IP address
        based on its geolocation.
        
        The IP must have latitude and longitude coordinates (run ip_to_infos first
        if the IP hasn't been geolocated).
        
        Note: IP geolocation is typically accurate to the city level, so results
        represent places in the general area rather than the exact location.
        
        You can filter results by category (e.g., datacenter, office, hotel) and
        adjust the search radius.
        """

    async def scan(self, data: list[InputType]) -> list[OutputType]:
        """Query Overture Maps for places near each IP's geolocation."""
        Logger.info(self.sketch_id, {"message": f"IP to Places scan started for {len(data)} items"})
        results: list[OutputType] = []
        client = get_overture_client()

        # Get parameters
        radius_km = float(self.params.get("radius_km", 5.0))
        limit = int(self.params.get("limit", 20))
        categories_raw = self.params.get("categories")
        categories_list = [c.strip() for c in categories_raw.split(",")] if categories_raw else None

        for ip in data:
            # Check if IP has coordinates
            if ip.latitude is None or ip.longitude is None:
                Logger.info(
                    self.sketch_id,
                    {
                        "message": f"IP {ip.address} missing coordinates. "
                        f"Run ip_to_infos first to geolocate."
                    },
                )
                continue

            try:
                # Query Overture Maps
                places_data = client.query_places_near_point(
                    latitude=ip.latitude,
                    longitude=ip.longitude,
                    radius_km=radius_km,
                    categories=categories_list,
                    limit=limit,
                )

                # Convert to Place objects
                for place_dict in places_data:
                    try:
                        place = Place(
                            name=place_dict["name"],
                            category=place_dict["category"],
                            latitude=place_dict["latitude"],
                            longitude=place_dict["longitude"],
                            address=place_dict.get("address"),
                            city=self._extract_city(place_dict.get("address")),
                            country=ip.country,  # Use IP's country
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
                        "message": f"Found {len(places_data)} places near IP {ip.address} "
                        f"({ip.city or 'unknown city'}, {ip.country or 'unknown country'})"
                    },
                )

            except Exception as e:
                Logger.error(
                    self.sketch_id,
                    {"message": f"Error querying Overture Maps for IP {ip.address}: {e}"},
                )
                continue

        return results

    def postprocess(
        self, results: list[OutputType], original_input: list[InputType]
    ) -> list[OutputType]:
        """Create graph nodes and relationships."""
        if not self._graph_service:
            return results

        # Create/update IP nodes
        for ip in original_input:
            if ip.latitude is not None and ip.longitude is not None:
                self.create_node(ip)

        # Create Place nodes and relationships
        for place in results:
            self.create_node(place)

            # Link to the nearest IP based on coordinates
            nearest_ip = self._find_nearest_ip(place, original_input)
            if nearest_ip:
                self.create_relationship(nearest_ip, place, "GEOLOCATES_NEAR")

            self.log_graph_message(
                f"Place near IP: {place.name} ({place.category})"
            )

        return results

    def _find_nearest_ip(self, place: Place, ips: list[Ip]) -> Optional[Ip]:
        """Find the nearest IP to a place based on coordinates."""
        import math

        min_distance = float("inf")
        nearest = None

        for ip in ips:
            if ip.latitude is None or ip.longitude is None:
                continue

            distance = math.sqrt(
                (place.latitude - ip.latitude) ** 2
                + (place.longitude - ip.longitude) ** 2
            )

            if distance < min_distance:
                min_distance = distance
                nearest = ip

        return nearest

    def _extract_city(self, address: Optional[str]) -> Optional[str]:
        """Try to extract city from address string."""
        if not address:
            return None
        # Simple heuristic: second part of comma-separated address is often city
        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 2:
            return parts[1]
        return None


# Export types
InputType = IpToPlacesEnricher.InputType
OutputType = IpToPlacesEnricher.OutputType
