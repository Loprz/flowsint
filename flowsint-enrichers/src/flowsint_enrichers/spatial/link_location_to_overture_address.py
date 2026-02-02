
"""
Link Location to Overture Address Enricher

Resolves a Location (lat/lon) to a specific Overture Address Object (GERS ID)
and links them via SAME_AS relation to "GERSify" the input data without altering user input.
Also bridges to containing Divisions for administrative context.
"""
from typing import Any, Dict, List, Optional
import logging
from shapely.geometry import shape, Point
from flowsint_enrichers import flowsint_enricher
from flowsint_core.core.enricher_base import Enricher
from flowsint_types import Location, Division
from tools.overture.client import get_overture_client

logger = logging.getLogger(__name__)

# Division hierarchy for bridging
DIVISION_HIERARCHY = ["locality", "county", "region", "country"]


@flowsint_enricher
class LinkLocationToOvertureAddress(Enricher):
    """Enricher that matches a Location to an Overture Address to assign a GERS ID."""

    InputType = Location
    OutputType = Location  # We output the *GERS* location

    @classmethod
    def name(cls) -> str:
        return "link_location_to_overture_address"

    @classmethod
    def category(cls) -> str:
        return "Spatial"

    @classmethod
    def key(cls) -> str:
        return "gers_address"

    async def scan(self, data: List[InputType]) -> List[OutputType]:
        results: List[Location] = []
        client = get_overture_client()

        for item in data:
            try:
                lat = getattr(item, "latitude", None)
                lon = getattr(item, "longitude", None)

                if lat is None or lon is None:
                    continue

                # Search radius ~50m (approx 0.0005 deg)
                radius_deg = 0.0005
                bbox = (lon - radius_deg, lat - radius_deg, lon + radius_deg, lat + radius_deg)

                # Fetch addresses
                addresses = client.query_addresses(bbox=bbox, limit=20)
                
                point = Point(lon, lat)
                best_match = None
                min_dist = float("inf")

                for addr_data in addresses:
                    geom = addr_data.get("geometry")
                    if not geom:
                        continue
                    
                    try:
                        addr_shape = shape(geom)
                        dist = addr_shape.distance(point)
                        
                        # Strict check: 20 meters (approx 0.0002 deg)
                        if dist < 0.0002 and dist < min_dist:
                            min_dist = dist
                            best_match = addr_data
                    except Exception as e:
                        logger.warning(f"Failed to calculate distance for address: {e}")
                        continue

                if best_match:
                    logger.info(f"GERSified Location Matches: {item.address} -> {best_match.get('id')} (dist={min_dist})")
                    
                    # Parse address components from Overture
                    street = best_match.get("street")
                    number = best_match.get("number")
                    postcode = best_match.get("postcode")
                    
                    full_address = f"{number} {street}" if number and street else item.address
                    
                    # Use precise Overture geometry
                    match_lat = lat
                    match_lon = lon
                    if best_match.get("geometry"):
                        try:
                            geom_dict = best_match.get("geometry")
                            if geom_dict.get("type") == "Point":
                                match_lon, match_lat = geom_dict.get("coordinates")
                            else:
                                shape_obj = shape(geom_dict)
                                match_lat = shape_obj.centroid.y
                                match_lon = shape_obj.centroid.x
                        except Exception:
                            pass

                    gers_location = Location(
                        address=full_address,
                        gers_id=best_match.get("id"),
                        city=item.city,
                        zip=postcode,
                        latitude=match_lat,
                        longitude=match_lon
                    )
                    
                    results.append(gers_location)
                    
                    # Update Graph: SAME_AS link
                    if self._graph_service:
                        self.create_node(gers_location)
                        self.create_relationship(item, gers_location, "SAME_AS")
                        
                        # Bridge to Divisions (WITHIN_DIVISION)
                        await self._bridge_to_divisions(gers_location, match_lat, match_lon, client)

            except Exception as e:
                logger.error(f"Error resolving GERS address: {e}")
                continue
                
        return results
    
    async def _bridge_to_divisions(
        self, 
        address: Location, 
        lat: float, 
        lon: float, 
        client
    ) -> None:
        """Query containing divisions and link the address to them."""
        try:
            # Larger bbox for divisions (~5km)
            bbox = (lon - 0.05, lat - 0.05, lon + 0.05, lat + 0.05)
            divisions_data = client.query_divisions(bbox=bbox, limit=30)
            
            point = Point(lon, lat)
            divisions_by_subtype: Dict[str, Division] = {}
            
            for div_data in divisions_data:
                geom = div_data.get("geometry")
                if not geom:
                    continue
                
                if shape(geom).contains(point):
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
                    
                    # Create node and link to address
                    self.create_node(div_model)
                    self.create_relationship(address, div_model, "WITHIN_DIVISION")
                    
                    if subtype in DIVISION_HIERARCHY:
                        divisions_by_subtype[subtype] = div_model
            
            # Also link divisions hierarchically
            for i, subtype in enumerate(DIVISION_HIERARCHY[:-1]):
                child = divisions_by_subtype.get(subtype)
                if not child:
                    continue
                for parent_subtype in DIVISION_HIERARCHY[i+1:]:
                    parent = divisions_by_subtype.get(parent_subtype)
                    if parent:
                        self.create_relationship(child, parent, "WITHIN_DIVISION")
                        break
                        
        except Exception as e:
            logger.warning(f"Error bridging address to divisions: {e}")
