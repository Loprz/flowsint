"""
Resolve Building Context Enricher

Finds the Overture Building that contains the given Location or Place.
"""
from typing import Any, Dict, List, Optional
import logging
from shapely.geometry import shape, Point
from flowsint_enrichers import flowsint_enricher
from flowsint_core.core.enricher_base import Enricher
from flowsint_types import Building, Location, Place
from tools.overture.client import get_overture_client

logger = logging.getLogger(__name__)


@flowsint_enricher
class ResolveBuilding(Enricher):
    """Enricher that resolves building context for a location."""

    InputType = Location
    OutputType = Building

    @classmethod
    def name(cls) -> str:
        return "resolve_building"

    @classmethod
    def category(cls) -> str:
        return "Context"
    
    @classmethod
    def key(cls) -> str:
        return "building"

    async def scan(self, data: List[InputType]) -> List[OutputType]:
        results: List[Building] = []
        client = get_overture_client()

        for item in data:
            try:
                lat = getattr(item, "latitude", None)
                lon = getattr(item, "longitude", None)

                if lat is None or lon is None:
                    continue

                buffer_deg = 0.0005
                bbox = (lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg)

                buildings_data = client.query_buildings(bbox=bbox, limit=50)
                logger.info(f"Checking {len(buildings_data)} buildings for containment at ({lat}, {lon})")
                
                point = Point(lon, lat)
                found_building = None
                
                # First: find containing building
                for b_data in buildings_data:
                    geom = b_data.get("geometry")
                    if not geom:
                        continue
                    try:
                        b_shape = shape(geom)
                        if b_shape.contains(point):
                            found_building = b_data
                            logger.info(f"Found containing building: {b_data.get('id')}")
                            break
                    except Exception as e:
                        logger.warning(f"Error checking containment: {e}")
                        continue
                
                # Fallback: nearest building within ~220m
                if not found_building and buildings_data:
                    logger.info("No containing building, checking nearest...")
                    nearest = None
                    min_dist = float("inf")
                    for b_data in buildings_data:
                        geom = b_data.get("geometry")
                        if not geom: continue
                        try:
                            dist = shape(geom).distance(point)
                            if dist < min_dist:
                                min_dist = dist
                                nearest = b_data
                        except:
                            continue
                    if nearest and min_dist < 0.002:
                        found_building = nearest
                        logger.info(f"Using nearest building: {nearest.get('id')} (dist={min_dist})")
                    else:
                        logger.info(f"Nearest building too far: dist={min_dist}")

                if found_building:
                    centroid_lat = lat
                    centroid_lon = lon
                    wkt_geom = None
                    try:
                        b_shape = shape(found_building["geometry"])
                        centroid = b_shape.centroid
                        centroid_lat = centroid.y
                        centroid_lon = centroid.x
                        wkt_geom = b_shape.wkt
                    except:
                        pass

                    name_str = "Building"
                    if found_building.get("class"):
                        name_str += f" ({found_building.get('class')})"

                    building = Building(
                        gers_id=found_building["id"],
                        height=found_building.get("height"),
                        levels=found_building.get("num_floors"),
                        type_class=found_building.get("class"),
                        latitude=centroid_lat,
                        longitude=centroid_lon,
                        name=name_str,
                        geometry=wkt_geom
                    )
                    results.append(building)
                    
                    if self._graph_service:
                        self.create_node(building)
                        self.create_relationship(item, building, "LOCATED_IN")
                         
            except Exception as e:
                logger.error(f"Error resolving building: {e}")
                continue
        return results
