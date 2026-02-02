"""
Routing API endpoints for pathfinding operations.

Uses Neo4j's APOC pathfinding algorithms (Dijkstra, A*) to calculate
shortest routes between locations in the road network.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from flowsint_core.core.graph import create_graph_service
from flowsint_core.core.graph.connection import Neo4jConnection
from flowsint_core.core.models import Profile

from app.api.deps import get_current_user

router = APIRouter()


class ShortestPathRequest(BaseModel):
    """Request body for shortest path calculation."""
    
    sketch_id: str = Field(..., description="Sketch ID containing the road network")
    origin_node_id: str = Field(..., description="Element ID of the origin location node")
    destination_node_id: str = Field(..., description="Element ID of the destination location node")
    algorithm: str = Field(
        default="dijkstra",
        description="Pathfinding algorithm: 'dijkstra' or 'astar'",
    )


class RoutePoint(BaseModel):
    """A point along the calculated route."""
    
    latitude: float
    longitude: float


class ShortestPathResponse(BaseModel):
    """Response containing the calculated route."""
    
    route: List[List[float]] = Field(..., description="Route as [[lat, lon], ...]")
    distance_m: float = Field(..., description="Total route distance in meters")
    intersection_count: int = Field(..., description="Number of intersections in the path")
    success: bool = Field(default=True)
    message: Optional[str] = None


@router.post("/shortest-path", response_model=ShortestPathResponse)
async def calculate_shortest_path(
    request: ShortestPathRequest,
    current_user: Profile = Depends(get_current_user),
):
    """
    Calculate the shortest path between two locations.
    
    The locations must have been linked to the road network via the
    NEAREST_INTERSECTION relationship (created by the LoadRoadNetwork enricher).
    """
    try:
        connection = Neo4jConnection.get_instance()
        
        # First, get the nearest intersections for both locations
        find_intersections_query = """
        MATCH (origin) WHERE elementId(origin) = $origin_id
        MATCH (dest) WHERE elementId(dest) = $dest_id
        OPTIONAL MATCH (origin)-[:NEAREST_INTERSECTION]->(source:Intersection)
        OPTIONAL MATCH (dest)-[:NEAREST_INTERSECTION]->(target:Intersection)
        RETURN 
            source.intersection_id AS source_id,
            source.latitude AS source_lat,
            source.longitude AS source_lon,
            target.intersection_id AS target_id,
            target.latitude AS target_lat,
            target.longitude AS target_lon
        """
        
        intersection_result = connection.query(
            find_intersections_query,
            {
                "origin_id": request.origin_node_id,
                "dest_id": request.destination_node_id,
            },
        )
        
        if not intersection_result:
            raise HTTPException(
                status_code=404,
                detail="Could not find origin or destination nodes",
            )
        
        row = intersection_result[0]
        source_id = row.get("source_id")
        target_id = row.get("target_id")
        
        if not source_id:
            raise HTTPException(
                status_code=400,
                detail="Origin location is not linked to the road network. Run the 'Load Road Network' enricher first.",
            )
        
        if not target_id:
            raise HTTPException(
                status_code=400,
                detail="Destination location is not linked to the road network. Run the 'Load Road Network' enricher first.",
            )
        
        # Now calculate the shortest path using APOC
        if request.algorithm == "astar":
            # A* uses point-based heuristic for distance estimation
            path_query = """
            MATCH (source:Intersection {intersection_id: $source_id})
            MATCH (target:Intersection {intersection_id: $target_id})
            CALL apoc.algo.aStarConfig(
                source, 
                target, 
                'ROAD_SEGMENT', 
                {
                    pointPropName: 'location',
                    weight: 'length'
                }
            ) YIELD path, weight
            RETURN 
                [n in nodes(path) | [n.latitude, n.longitude]] AS route,
                weight AS total_distance,
                length(nodes(path)) AS intersection_count
            """
        else:
            # Default to Dijkstra's algorithm
            path_query = """
            MATCH (source:Intersection {intersection_id: $source_id})
            MATCH (target:Intersection {intersection_id: $target_id})
            CALL apoc.algo.dijkstra(source, target, 'ROAD_SEGMENT', 'length') 
            YIELD path, weight
            RETURN 
                [n in nodes(path) | [n.latitude, n.longitude]] AS route,
                weight AS total_distance,
                length(nodes(path)) AS intersection_count
            """
        
        path_result = connection.query(
            path_query,
            {
                "source_id": source_id,
                "target_id": target_id,
            },
        )
        
        if not path_result:
            return ShortestPathResponse(
                route=[],
                distance_m=0,
                intersection_count=0,
                success=False,
                message="No path found between the specified locations. The road network may be incomplete.",
            )
        
        path_row = path_result[0]
        route = path_row.get("route", [])
        total_distance = path_row.get("total_distance", 0)
        intersection_count = path_row.get("intersection_count", 0)
        
        return ShortestPathResponse(
            route=route,
            distance_m=total_distance,
            intersection_count=intersection_count,
            success=True,
            message=f"Route found with {intersection_count} intersections",
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating route: {str(e)}",
        )


@router.get("/check-network/{sketch_id}")
async def check_road_network(
    sketch_id: str,
    current_user: Profile = Depends(get_current_user),
):
    """
    Check the status of the road network for a sketch.
    
    Returns counts of intersections, segments, and linked locations.
    """
    try:
        connection = Neo4jConnection.get_instance()
        
        stats_query = """
        MATCH (i:Intersection)
        WHERE i.sketch_id = $sketch_id OR i.sketch_id IS NULL
        WITH count(i) AS intersection_count
        
        MATCH ()-[r:ROAD_SEGMENT]-()
        WITH intersection_count, count(r) / 2 AS segment_count
        
        MATCH (loc)-[:NEAREST_INTERSECTION]->(:Intersection)
        WHERE loc.sketch_id = $sketch_id
        WITH intersection_count, segment_count, count(loc) AS linked_locations
        
        RETURN intersection_count, segment_count, linked_locations
        """
        
        result = connection.query(stats_query, {"sketch_id": sketch_id})
        
        if not result:
            return {
                "intersection_count": 0,
                "segment_count": 0,
                "linked_locations": 0,
                "has_network": False,
            }
        
        row = result[0]
        intersection_count = row.get("intersection_count", 0)
        segment_count = row.get("segment_count", 0)
        linked_locations = row.get("linked_locations", 0)
        
        return {
            "intersection_count": intersection_count,
            "segment_count": segment_count,
            "linked_locations": linked_locations,
            "has_network": intersection_count > 0 and segment_count > 0,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error checking road network: {str(e)}",
        )
