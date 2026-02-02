"""
Resolve Divisions Enricher

Finds the Overture Divisions (administrative boundaries) that contain the given Location or Place.
Links divisions hierarchically (locality -> region -> country).
"""
from typing import Any, Dict, List, Optional
import logging
from shapely.geometry import shape, Point, mapping
from flowsint_enrichers import flowsint_enricher
from flowsint_core.core.enricher_base import Enricher
from flowsint_types import Division, Location, Place, Building
from tools.overture.client import get_overture_client

logger = logging.getLogger(__name__)

# Define hierarchy ordering for division linking (smaller -> larger)
DIVISION_HIERARCHY = ["locality", "county", "region", "country"]


@flowsint_enricher
class ResolveDivision(Enricher):
    """Enricher that resolves administrative divisions for a location."""

    InputType = Location
    OutputType = Division

    @classmethod
    def name(cls) -> str:
        return "resolve_division"

    @classmethod
    def category(cls) -> str:
        return "Context"

    @classmethod
    def key(cls) -> str:
        return "division"

    async def scan(self, data: List[InputType]) -> List[OutputType]:
        results: List[Division] = []
        client = get_overture_client()

        for item in data:
            try:
                lat = getattr(item, "latitude", None)
                lon = getattr(item, "longitude", None)

                if lat is None or lon is None:
                    continue

                # Box ~5km
                bbox = (lon - 0.05, lat - 0.05, lon + 0.05, lat + 0.05)
                divisions_data = client.query_divisions(bbox=bbox, limit=50)
                
                point = Point(lon, lat)
                
                # Collect containing divisions by subtype
                divisions_by_subtype: Dict[str, Division] = {}
                
                for div_data in divisions_data:
                    geom = div_data.get("geometry")
                    if not geom:
                        continue
                        
                    if shape(geom).contains(point):
                        # Create Model
                        subtype = div_data.get("subtype", "unknown")
                        name = "Unknown"
                        names = div_data.get("names", {})
                        if names and "primary" in names:
                            name = names["primary"]
                        elif names and "common" in names:
                            common = names["common"]
                            name = common[0]["value"] if isinstance(common, list) and common else "Unknown"

                        div_model = Division(
                            gers_id=div_data["id"],
                            division_id=div_data.get("id"),
                            name=name,
                            subtype=subtype,
                            country_iso=div_data.get("country_iso"),
                            latitude=lat, 
                            longitude=lon 
                        )
                        results.append(div_model)
                        
                        # Track for hierarchy linking
                        if subtype in DIVISION_HIERARCHY:
                            divisions_by_subtype[subtype] = div_model
                        
                        if self._graph_service:
                            self.create_node(div_model)
                            self.create_relationship(item, div_model, "WITHIN_DIVISION")
                
                # Link divisions hierarchically: locality -> county -> region -> country
                if self._graph_service:
                    self._link_division_hierarchy(divisions_by_subtype)

            except Exception as e:
                logger.error(f"Error resolving divisions: {e}")
                continue
                
        return results
    
    def _link_division_hierarchy(self, divisions_by_subtype: Dict[str, "Division"]) -> None:
        """Create WITHIN_DIVISION edges between divisions in the hierarchy."""
        for i, subtype in enumerate(DIVISION_HIERARCHY[:-1]):
            child = divisions_by_subtype.get(subtype)
            if not child:
                continue
            
            # Find the next larger division in hierarchy
            for parent_subtype in DIVISION_HIERARCHY[i+1:]:
                parent = divisions_by_subtype.get(parent_subtype)
                if parent:
                    self.create_relationship(child, parent, "WITHIN_DIVISION")
                    logger.debug(f"Linked {child.name} ({subtype}) -> {parent.name} ({parent_subtype})")
                    break  # Only link to immediate parent
