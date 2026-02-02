"""
Load Road Network Enricher

Loads Overture Maps transportation data (road segments and connectors)
into Neo4j for routing and network analysis.

Provides two enrichers:
- LoadRoadNetworkFromLocation: For Location nodes (addresses)
- LoadRoadNetworkFromPlace: For Place nodes (POIs)
"""
from __future__ import annotations
from typing import Any, List, Optional, Union
from pydantic import BaseModel, Field

from flowsint_core.core.enricher_base import Enricher
from flowsint_core.core.logger import Logger
from flowsint_enrichers.registry import flowsint_enricher
from flowsint_types import Location, Place, Intersection, RoadSegment

# Import Overture client
try:
    from tools.overture.client import get_overture_client
except ImportError:
    import sys
    import os
    tools_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "tools")
    sys.path.insert(0, tools_path)
    from overture.client import get_overture_client


class LoadRoadNetworkParams(BaseModel):
    """Parameters for the LoadRoadNetwork enricher."""

    radius_km: float = Field(
        default=2.0,
        description="Search radius in kilometers around each location",
        ge=0.5,
        le=10.0,
    )
    road_classes: Optional[str] = Field(
        default=None,
        description="Comma-separated list of road classes to load (e.g., 'primary,secondary,tertiary'). Leave empty for all.",
    )


class BaseLoadRoadNetworkEnricher(Enricher):
    """Base class for road network enrichers with shared logic."""

    OutputType = Intersection

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
                "description": "Search radius in kilometers around each location",
                "required": False,
                "default": 2.0,
            },
            {
                "name": "road_classes",
                "type": "string",
                "description": "Comma-separated road classes (e.g., 'primary,secondary'). Leave empty for all.",
                "required": False,
            },
        ]

    @classmethod
    def key(cls) -> str:
        return "address"

    @classmethod
    def documentation(cls) -> str:
        return """
        This enricher loads road network data from Overture Maps into Neo4j.
        
        It creates Intersection nodes and ROAD_SEGMENT relationships that can be
        used with Neo4j's APOC pathfinding algorithms (Dijkstra, A*) to calculate
        routes between locations.
        
        The enricher queries the road network around each input location/place and
        imports it into the graph.
        """

    def _get_coordinates(self, item: Any) -> tuple[Optional[float], Optional[float]]:
        """Get latitude and longitude from an input item."""
        return getattr(item, "latitude", None), getattr(item, "longitude", None)

    def _get_label(self, item: Any) -> str:
        """Get a label for logging from an input item."""
        if hasattr(item, "address") and item.address:
            return item.address
        if hasattr(item, "name") and item.name:
            return item.name
        lat, lon = self._get_coordinates(item)
        if lat is not None and lon is not None:
            return f"{lat:.4f}, {lon:.4f}"
        return "unknown"

    async def scan(self, data: list) -> list[Intersection]:
        """Load road network data around each input item."""
        Logger.info(self.sketch_id, {"message": f"Road network load started for {len(data)} items"})
        
        client = get_overture_client()

        # Get parameters
        radius_km = float(self.params.get("radius_km", 2.0))
        road_classes_raw = self.params.get("road_classes")
        
        road_class_filter = None
        if road_classes_raw and road_classes_raw.strip():
            road_class_filter = [c.strip() for c in road_classes_raw.split(",")]

        Logger.info(self.sketch_id, {"message": f"Parameters: radius={radius_km}km, road_classes={road_class_filter or 'all'}"})

        # Collect all unique connectors and segments
        all_connectors: dict[str, dict] = {}
        all_segments: list[dict] = []

        for item in data:
            lat, lon = self._get_coordinates(item)
            if lat is None or lon is None:
                Logger.info(self.sketch_id, {"message": f"Skipping {self._get_label(item)} - missing coordinates"})
                continue

            try:
                Logger.info(self.sketch_id, {"message": f"Querying road network near {lat}, {lon}"})
                
                transport_data = client.query_transportation_near_point(
                    latitude=lat,
                    longitude=lon,
                    radius_km=radius_km,
                    road_class_filter=road_class_filter,
                )

                # Deduplicate connectors
                for conn in transport_data.get("connectors", []):
                    if conn["id"] not in all_connectors:
                        all_connectors[conn["id"]] = conn

                # Collect segments
                all_segments.extend(transport_data.get("segments", []))

                Logger.info(
                    self.sketch_id,
                    {"message": f"Found {len(transport_data.get('connectors', []))} connectors, {len(transport_data.get('segments', []))} segments near '{self._get_label(item)}'"},
                )

            except Exception as e:
                Logger.error(
                    self.sketch_id,
                    {"message": f"Error querying road network: {e}"},
                )
                continue

        Logger.info(
            self.sketch_id,
            {"message": f"Total unique connectors: {len(all_connectors)}, segments: {len(all_segments)}"},
        )

        # Convert to Intersection objects for return
        results = []
        for conn_id, conn in all_connectors.items():
            intersection = Intersection(
                intersection_id=conn["id"],
                latitude=conn["latitude"],
                longitude=conn["longitude"],
                source="overture",
            )
            results.append(intersection)

        # Store segments in instance for postprocess
        self._segments = all_segments
        self._connectors = all_connectors

        return results

    def postprocess(
        self, results: list[Intersection], original_input: list
    ) -> list[Intersection]:
        """Create graph nodes and relationships."""
        if not self._graph_service:
            return results

        # Create Intersection nodes
        for intersection in results:
            self._create_intersection_node(intersection)

        # Create ROAD_SEGMENT relationships
        segments_created = 0
        for seg in getattr(self, "_segments", []):
            try:
                self._create_segment_relationship(seg)
                segments_created += 1
            except Exception as e:
                Logger.debug(self.sketch_id, {"message": f"Error creating segment: {e}"})

        self.log_graph_message(
            f"Road network loaded: {len(results)} intersections, {segments_created} road segments"
        )

        # Link original items to their nearest intersection
        for item in original_input:
            lat, lon = self._get_coordinates(item)
            if lat is None or lon is None:
                continue
            nearest = self._find_nearest_intersection(lat, lon, results)
            if nearest:
                self.create_relationship(item, nearest, "NEAREST_INTERSECTION")

        return results

    def _create_intersection_node(self, intersection: Intersection) -> None:
        """Create an Intersection node in Neo4j using raw Cypher."""
        if not self._graph_service:
            return

        try:
            # Use raw Cypher query via the repository's connection
            connection = self._graph_service.repository._connection
            query = """
            MERGE (i:Intersection {intersection_id: $intersection_id})
            ON CREATE SET 
                i.latitude = $latitude,
                i.longitude = $longitude,
                i.source = $source
            ON MATCH SET 
                i.latitude = $latitude,
                i.longitude = $longitude,
                i.source = $source
            """
            connection.execute_write(query, {
                "intersection_id": intersection.intersection_id,
                "latitude": intersection.latitude,
                "longitude": intersection.longitude,
                "source": intersection.source or "overture",
            })
        except Exception as e:
            Logger.debug(self.sketch_id, {"message": f"Error creating intersection node: {e}"})

    def _create_segment_relationship(self, segment: dict) -> None:
        """Create a ROAD_SEGMENT relationship in Neo4j using raw Cypher."""
        if not self._graph_service:
            return

        try:
            # Use raw Cypher query via the repository's connection
            connection = self._graph_service.repository._connection
            query = """
            MATCH (start:Intersection {intersection_id: $start_id})
            MATCH (end:Intersection {intersection_id: $end_id})
            MERGE (start)-[r:ROAD_SEGMENT {segment_id: $segment_id}]->(end)
            ON CREATE SET 
                r.name = $name,
                r.road_class = $road_class,
                r.length = $length
            ON MATCH SET 
                r.name = $name,
                r.road_class = $road_class,
                r.length = $length
            """
            connection.execute_write(query, {
                "start_id": segment["start_connector"],
                "end_id": segment["end_connector"],
                "segment_id": segment["id"],
                "name": segment.get("name"),
                "road_class": segment.get("road_class"),
                "length": segment.get("length_m", 0),
            })
        except Exception as e:
            Logger.debug(self.sketch_id, {"message": f"Error creating road segment: {e}"})

    def _find_nearest_intersection(
        self, lat: float, lon: float, intersections: list[Intersection]
    ) -> Optional[Intersection]:
        """Find the nearest intersection to a coordinate."""
        import math

        min_distance = float("inf")
        nearest = None

        for intersection in intersections:
            distance = math.sqrt(
                (lat - intersection.latitude) ** 2
                + (lon - intersection.longitude) ** 2
            )
            if distance < min_distance:
                min_distance = distance
                nearest = intersection

        return nearest


@flowsint_enricher
class LoadRoadNetworkFromLocation(BaseLoadRoadNetworkEnricher):
    """[Overture Maps] Load road network around locations for routing."""

    InputType = Location

    @classmethod
    def name(cls) -> str:
        return "load_road_network_location"

    @classmethod
    def category(cls) -> str:
        return "Location"


@flowsint_enricher
class LoadRoadNetworkFromPlace(BaseLoadRoadNetworkEnricher):
    """[Overture Maps] Load road network around places for routing."""

    InputType = Place

    @classmethod
    def name(cls) -> str:
        return "load_road_network_place"

    @classmethod
    def category(cls) -> str:
        return "Place"


# For backwards compatibility
LoadRoadNetworkEnricher = LoadRoadNetworkFromPlace
InputType = Place
OutputType = Intersection
