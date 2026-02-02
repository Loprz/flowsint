"""
Link Place to Address Enricher

Links a Place node to the nearest Overture Address node.
"""
from typing import Any, Dict, List, Optional
import logging
from shapely.geometry import shape, Point
from flowsint_enrichers import flowsint_enricher
from flowsint_core.core.enricher_base import Enricher
from flowsint_types import Place, Location
from tools.overture.client import get_overture_client

logger = logging.getLogger(__name__)


@flowsint_enricher
class LinkPlaceToAddress(Enricher):
    """Enricher that links a Place to a nearby Address."""

    InputType = Place
    OutputType = Location

    @classmethod
    def name(cls) -> str:
        return "link_place_to_address"

    @classmethod
    def category(cls) -> str:
        return "Spatial"

    @classmethod
    def key(cls) -> str:
        return "address"

    async def scan(self, data: List[InputType]) -> List[OutputType]:
        results: List[Location] = []
        client = get_overture_client()

        for item in data:
            try:
                lat = getattr(item, "latitude", None)
                lon = getattr(item, "longitude", None)

                if lat is None or lon is None:
                    continue

                buffer_deg = 0.0005
                bbox = (lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg)
                
                addresses = client.query_addresses(bbox=bbox, limit=20)
                if not addresses:
                    continue

                point = Point(lon, lat)
                nearest_addr = None
                min_dist = float("inf")

                for addr in addresses:
                    geom = addr.get("geometry")
                    if not geom:
                         continue
                    if shape(geom).distance(point) < min_dist:
                        min_dist = shape(geom).distance(point)
                        nearest_addr = addr

                if nearest_addr:
                    addr_model = Location(
                        gers_id=nearest_addr["id"],
                        address=f"{nearest_addr.get('number', '')} {nearest_addr.get('street', '')}".strip(),
                        zip=nearest_addr.get("postcode"),
                        latitude=shape(nearest_addr["geometry"]).y,
                        longitude=shape(nearest_addr["geometry"]).x
                    )
                    results.append(addr_model)

                    if self._graph_service:
                        self.create_node(addr_model)
                        self.create_relationship(item, addr_model, "HAS_ADDRESS")

            except Exception as e:
                logger.error(f"Error linking place to address: {e}")
                continue
        
        return results
