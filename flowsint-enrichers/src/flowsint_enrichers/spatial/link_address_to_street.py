"""
Link Address to Street Enricher

Links a Location (Address) to the nearest Overture Road Segment.
"""
from typing import Any, Dict, List, Optional
import logging
from shapely.geometry import shape, Point, mapping
from shapely.ops import nearest_points
from flowsint_enrichers import flowsint_enricher
from flowsint_core.core.enricher_base import Enricher
from flowsint_types import Location, RoadSegment
from tools.overture.client import get_overture_client

logger = logging.getLogger(__name__)


@flowsint_enricher
class LinkAddressToStreet(Enricher):
    """Enricher that links an address to the nearest road segment."""

    InputType = Location
    OutputType = RoadSegment

    @classmethod
    def name(cls) -> str:
        return "link_address_to_street"

    @classmethod
    def category(cls) -> str:
        return "Spatial"

    @classmethod
    def key(cls) -> str:
        return "street"

    async def scan(self, data: List[InputType]) -> List[OutputType]:
        results: List[RoadSegment] = []
        client = get_overture_client()

        for item in data:
            try:
                lat = getattr(item, "latitude", None)
                lon = getattr(item, "longitude", None)

                if lat is None or lon is None:
                    continue

                resp = client.query_transportation_near_point(
                    latitude=lat, longitude=lon, radius_km=1.0
                )
                segments = resp.get("segments", [])
                if not segments:
                    continue

                point = Point(lon, lat)
                nearest_segment = None
                min_dist = float("inf")

                for seg in segments:
                    geom = seg.get("geometry")
                    if not geom:
                        continue
                    if shape(geom).distance(point) < min_dist:
                        min_dist = shape(geom).distance(point)
                        nearest_segment = seg

                if nearest_segment:
                    road = RoadSegment(
                        gers_id=nearest_segment["id"],
                        name=str(nearest_segment.get("names", {}).get("primary", "Unnamed Road")),
                        road_class=nearest_segment.get("class", "unknown"),
                        latitude=lat, # associating with search point
                        longitude=lon
                    )
                    results.append(road)

                    if self._graph_service:
                        self.create_node(road)
                        self.create_relationship(item, road, "LOCATED_ON")

            except Exception as e:
                logger.error(f"Error linking street: {e}")
                continue
                
        return results
