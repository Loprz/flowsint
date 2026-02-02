"""
Overture Maps Client

A client for querying Overture Maps data. This uses the overturemaps-py library
which provides access to Overture Maps data without downloading the full dataset.

Usage:
    client = OvertureMapsClient()
    places = client.query_places(
        bbox=(-77.05, 38.89, -77.03, 38.91),  # DC area
        categories=["restaurant", "hotel"],
        limit=50
    )
"""

from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class OvertureMapsClient:
    """Client for querying Overture Maps data."""

    def __init__(self):
        """Initialize the Overture Maps client."""
        self._overture = None
        self._initialized = False

    def _ensure_initialized(self) -> bool:
        """Lazily initialize the overturemaps library."""
        if self._initialized:
            return self._overture is not None

        try:
            import overturemaps
            self._overture = overturemaps
            self._initialized = True
            logger.info("Overture Maps client initialized successfully")
            return True
        except ImportError:
            logger.warning(
                "overturemaps package not installed. "
                "Install with: pip install overturemaps"
            )
            self._initialized = True
            return False

    def query_places(
        self,
        bbox: tuple,
        categories: Optional[List[str]] = None,
        limit: int = 50,
        center_point: Optional[tuple] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query places from Overture within a bounding box.

        Args:
            bbox: (min_lon, min_lat, max_lon, max_lat)
            categories: Optional list of category filters
            limit: Maximum number of results to return
            center_point: Optional (lat, lon) to sort results by proximity

        Returns:
            List of place dictionaries with name, category, coordinates, etc.
        """
        if not self._ensure_initialized():
            logger.error("Overture Maps not available, returning empty results")
            return []

        try:
            import math
            # Query places theme from Overture Maps
            reader = self._overture.record_batch_reader(
                "place",
                bbox=bbox,
            )
            gdf = reader.read_all().to_pandas()

            logger.info(f"Overture raw search returned {len(gdf)} records in bbox {bbox}")
            if len(gdf) > 0:
                logger.debug(f"Overture columns: {gdf.columns.tolist()}")

            results = []
            filtered_count = 0
            
            # If a center point is provided, we should probably parse more records to find the closest ones
            # but limit the total parsing to avoid extreme wait times
            parse_limit = 5000 if center_point else limit * 4
            
            logger.info(f"Overture: filtering up to {len(gdf)} raw records (parsing max {parse_limit}) with categories={categories}")
            
            for idx, row in gdf.iterrows():
                if len(results) >= parse_limit:
                    break
                    
                place = self._parse_place_row(row)
                if place:
                    # Apply category filter if specified
                    if categories and len(categories) > 0:
                        place_category = place.get("category", "").lower()
                        if not any(cat.lower() in place_category for cat in categories):
                            filtered_count += 1
                            continue
                    
                    # Calculate distance if center_point is provided
                    if center_point:
                        c_lat, c_lon = center_point
                        p_lat, p_lon = place["latitude"], place["longitude"]
                        # Simple Euclidean distance for sorting (sufficient for POIs)
                        dist = math.sqrt((p_lat - c_lat)**2 + (p_lon - c_lon)**2)
                        place["_distance"] = dist

                    results.append(place)

            # Sort by distance if center point provided
            if center_point:
                results.sort(key=lambda x: x.get("_distance", 0))
                # Remove internal helper field
                for r in results:
                    r.pop("_distance", None)

            logger.info(f"Overture search results: {len(results)} matches, {filtered_count} records filtered by category, {len(gdf) - len(results) - filtered_count} records skipped or invalid")
            
            if len(results) == 0 and len(gdf) > 0:
                logger.info(f"Overture: No results found among {len(gdf)} records. First record ID: {gdf.iloc[0].get('id')} Names: {gdf.iloc[0].get('names')}")

            return results[:limit]

        except Exception as e:
            logger.error(f"Error querying Overture Maps: {e}")
            return []

    def query_places_near_point(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 1.0,
        categories: Optional[List[str]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Query places near a specific point.

        Args:
            latitude: Center latitude
            longitude: Center longitude
            radius_km: Search radius in kilometers
            categories: Optional category filters
            limit: Maximum results

        Returns:
            List of place dictionaries
        """
        # Convert radius to approximate bbox
        # 1 degree latitude ≈ 111 km
        # 1 degree longitude ≈ 111 km * cos(latitude)
        import math
        
        lat_delta = radius_km / 111.0
        lon_delta = radius_km / (111.0 * math.cos(math.radians(latitude)))

        bbox = (
            longitude - lon_delta,  # min_lon
            latitude - lat_delta,   # min_lat
            longitude + lon_delta,  # max_lon
            latitude + lat_delta,   # max_lat
        )

        return self.query_places(
            bbox=bbox, 
            categories=categories, 
            limit=limit,
            center_point=(latitude, longitude)
        )

    def _parse_place_row(self, row: Any) -> Optional[Dict[str, Any]]:
        """Parse a GeoDataFrame row into a place dictionary."""
        try:
            import numpy as np
            from shapely import wkb
            import traceback
            
            row_id = row.get("id", "unknown")

            def to_py(val):
                """Force Arrow/NumPy objects to plain Python."""
                if val is None: return None
                if hasattr(val, "as_py"): return val.as_py()
                if isinstance(val, np.ndarray): return val.tolist()
                return val

            def ensure_dict(val):
                val = to_py(val)
                if isinstance(val, list) and len(val) > 0:
                    val = val[0]
                return val if isinstance(val, dict) else {}

            # 1. Geometry extraction
            geom = row.get("geometry")
            if geom is None:
                logger.info(f"Row {row_id} skip: geometry is None")
                return None

            if isinstance(geom, bytes):
                try:
                    geom = wkb.loads(geom)
                except Exception as e:
                    logger.info(f"Row {row_id} skip: WKB parse failed: {e}")
                    return None

            # Extract coordinates
            if hasattr(geom, "centroid"):
                centroid = geom.centroid
                lon, lat = centroid.x, centroid.y
            elif hasattr(geom, "x") and hasattr(geom, "y"):
                lon, lat = geom.x, geom.y
            else:
                logger.info(f"Row {row_id} skip: geom has no x/y or centroid. Type: {type(geom)}")
                return None

            # 2. Name extraction
            names = ensure_dict(row.get("names", {}))
            primary_name = names.get("primary") or names.get("common")
            
            if not primary_name and isinstance(row.get("names"), str):
                primary_name = row.get("names")

            if not primary_name:
                logger.info(f"Row {row_id} skip: no primary name found in {names}")
                return None

            # 3. Category extraction
            cats = ensure_dict(row.get("categories", {}))
            primary_category = cats.get("primary", "unknown")
            
            if primary_category == "unknown" and isinstance(row.get("categories"), str):
                primary_category = row.get("categories")

            # 4. Address extraction
            address_str = None
            addrs = to_py(row.get("addresses", []))
            if isinstance(addrs, list) and len(addrs) > 0:
                addr = addrs[0]
                if isinstance(addr, dict):
                    parts = [
                        addr.get("freeform", ""),
                        addr.get("locality", ""),
                        addr.get("region", ""),
                        addr.get("country", ""),
                    ]
                    address_str = ", ".join(p for p in parts if p)

            # Standardize lists for websites, phones, socials
            def to_list(val):
                val = to_py(val)
                if val is None: return []
                if isinstance(val, list): return [str(v) for v in val if v]
                return [str(val)]

            websites = to_list(row.get("websites"))
            phones = to_list(row.get("phones"))
            socials = to_list(row.get("socials"))
            
            # Append from sources if available
            sources = to_py(row.get("sources", []))
            if isinstance(sources, list):
                for src in sources:
                    if isinstance(src, dict):
                        if "website" in src: websites.append(src["website"])
                        if "phone" in src: phones.append(src["phone"])

            # Build the place dict
            place = {
                "name": str(primary_name),
                "category": str(primary_category),
                "latitude": float(lat),
                "longitude": float(lon),
                "overture_id": str(row_id),
                "confidence": float(to_py(row.get("confidence"))) if row.get("confidence") is not None else None,
                "source": self._extract_source(row.get("sources")),
                "address": address_str,
                "brand": str(ensure_dict(row.get("brand")).get("names", {}).get("primary", "")) or None,
                "websites": list(set(websites)) if websites else None,
                "phones": list(set(phones)) if phones else None,
                "socials": list(set(socials)) if socials else None,
            }

            return place

        except Exception as e:
            logger.info(f"Row {row.get('id', 'unknown')} skip: Critical parsing error: {e}\n{traceback.format_exc()}")
            return None

    def _extract_source(self, sources: Any) -> Optional[str]:
        """Extract primary data source from sources array."""
        try:
            if sources is None:
                return None
            
            # Handle NumPy arrays or lists - check length instead of truthiness
            if not hasattr(sources, "__len__") or len(sources) == 0:
                return None

            first_source = sources[0]
            if hasattr(first_source, "get"):
                # Use as_py() if it's an Arrow scalar or similar
                dataset = first_source.get("dataset")
                source = first_source.get("source")
                if hasattr(dataset, "as_py"): dataset = dataset.as_py()
                if hasattr(source, "as_py"): source = source.as_py()
                return str(dataset or source)
            
            return str(first_source)
        except Exception as e:
            logger.debug(f"Error extracting source: {e}")
            return None

    def query_transportation(
        self,
        bbox: Tuple[float, float, float, float],
        road_class_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Query Overture Maps for transportation data (road segments and connectors).

        Args:
            bbox: Bounding box as (min_lon, min_lat, max_lon, max_lat)
            road_class_filter: Optional list of road classes to include
                               (e.g., ["primary", "secondary", "tertiary"])

        Returns:
            Dict with 'segments' and 'connectors' lists
        """
        if not self._ensure_initialized():
            logger.error("Overture Maps not available, returning empty results")
            return {"segments": [], "connectors": []}

        try:
            import numpy as np
            from shapely import wkb

            # Query segments (road center-lines)
            segment_reader = self._overture.record_batch_reader(
                "segment",
                bbox=bbox,
            )
            segments_df = segment_reader.read_all().to_pandas()
            logger.info(f"Overture transportation: {len(segments_df)} raw segments in bbox {bbox}")

            # Query connectors (intersection points)
            connector_reader = self._overture.record_batch_reader(
                "connector",
                bbox=bbox,
            )
            connectors_df = connector_reader.read_all().to_pandas()
            logger.info(f"Overture transportation: {len(connectors_df)} raw connectors in bbox {bbox}")

            # Parse connectors (simpler - just points)
            connectors = []
            for _, row in connectors_df.iterrows():
                try:
                    conn_id = row.get("id")
                    geom = row.get("geometry")
                    
                    if geom is None or conn_id is None:
                        continue
                    
                    # Parse WKB geometry
                    if isinstance(geom, bytes):
                        geom = wkb.loads(geom)
                    
                    if hasattr(geom, "x") and hasattr(geom, "y"):
                        connectors.append({
                            "id": str(conn_id),
                            "latitude": float(geom.y),
                            "longitude": float(geom.x),
                        })
                except Exception as e:
                    logger.debug(f"Error parsing connector: {e}")
                    continue

            logger.info(f"Parsed {len(connectors)} connectors")

            # Parse segments (road lines)
            segments = []
            for _, row in segments_df.iterrows():
                try:
                    seg_id = row.get("id")
                    geom = row.get("geometry")
                    
                    if geom is None or seg_id is None:
                        continue
                    
                    # Parse WKB geometry
                    if isinstance(geom, bytes):
                        geom = wkb.loads(geom)
                    
                    # Get connector references
                    connector_ids = row.get("connectors", [])
                    if hasattr(connector_ids, "tolist"):
                        connector_ids = connector_ids.tolist()
                    elif hasattr(connector_ids, "as_py"):
                        connector_ids = connector_ids.as_py()
                    
                    if not connector_ids or len(connector_ids) < 2:
                        continue  # Need at least start and end connectors
                    
                    # Get road properties
                    road_class = None
                    road = row.get("road")
                    if road is not None:
                        if hasattr(road, "as_py"):
                            road = road.as_py()
                        if isinstance(road, dict):
                            road_class = road.get("class")
                    
                    # Apply road class filter
                    if road_class_filter and road_class not in road_class_filter:
                        continue
                    
                    # Extract road names
                    names = row.get("names", {})
                    if hasattr(names, "as_py"):
                        names = names.as_py()
                    primary_name = None
                    if isinstance(names, dict):
                        primary_name = names.get("primary")
                    
                    # Calculate segment length from geometry
                    length_m = 0.0
                    if hasattr(geom, "length"):
                        # Approximate: degrees to meters (very rough)
                        length_m = geom.length * 111000
                    
                    # Get coordinates for the polyline
                    coords = []
                    if hasattr(geom, "coords"):
                        coords = [[float(c[1]), float(c[0])] for c in geom.coords]  # [lat, lon]
                    
                    segments.append({
                        "id": str(seg_id),
                        "name": str(primary_name) if primary_name else None,
                        "road_class": str(road_class) if road_class else "unknown",
                        "length_m": length_m,
                        "start_connector": str(connector_ids[0]),
                        "end_connector": str(connector_ids[-1]),
                        "connector_ids": [str(c) for c in connector_ids],
                        "coordinates": coords,
                    })
                except Exception as e:
                    logger.debug(f"Error parsing segment: {e}")
                    continue

            logger.info(f"Parsed {len(segments)} segments")

            return {
                "segments": segments,
                "connectors": connectors,
            }

        except Exception as e:
            logger.error(f"Error querying Overture transportation: {e}")
            return {"segments": [], "connectors": []}

    def query_buildings(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query Overture Maps building data.
        
        Args:
            bbox: Bounding box as (min_x, min_y, max_x, max_y)
            limit: Maximum number of results
            
        Returns:
            List of building features
        """
        if not self._ensure_initialized():
            return []

        try:
            logger.info(f"Querying Overture buildings with bbox={bbox}")
            
            # Use overturemaps to fetch building data
            # Note: theme='buildings', type='building'
            record_iter = self._overture.record_batch_reader(
                "building", bbox=bbox
            )
            
            from shapely import wkb
            from shapely.geometry import mapping
            
            buildings = []
            for batch in record_iter:
                for row in batch.to_pylist():
                    if len(buildings) >= limit:
                        break
                    
                    geom = row.get("geometry")
                    # Handle WKB bytes if necessary
                    if isinstance(geom, bytes):
                        try:
                            geom_obj = wkb.loads(geom)
                            geom = mapping(geom_obj)
                        except Exception as e:
                            logger.warning(f"Building geometry parse error: {e}")
                            continue
                    
                    # Parse building data
                    building = {
                        "id": row.get("id"),
                        "names": row.get("names", {}),
                        "height": row.get("height"),
                        "num_floors": row.get("num_floors"),
                        "class": row.get("class"),
                        "geometry": geom,
                        "sources": row.get("sources", []),
                    }
                    buildings.append(building)
                
                if len(buildings) >= limit:
                    break
                    
            logger.info(f"Found {len(buildings)} buildings")
            return buildings
            
        except Exception as e:
            logger.error(f"Error querying Overture buildings: {e}")
            return []

    def query_addresses(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query Overture Maps addresses.
        
        Args:
            bbox: Bounding box
            limit: Max results
            
        Returns:
            List of address features
        """
        if not self._ensure_initialized():
            return []

        try:
            logger.info(f"Querying Overture addresses with bbox={bbox}")
            # theme='addresses', type='address' usually, check Overture schema if needed
            record_iter = self._overture.record_batch_reader(
                "address", bbox=bbox
            )
            
            from shapely import wkb

            addresses = []
            for batch in record_iter:
                for row in batch.to_pylist():
                    if len(addresses) >= limit:
                        break
                    
                    geom = row.get("geometry")
                    # Handle WKB bytes if necessary
                    if isinstance(geom, bytes):
                        try:
                            # Convert to shapely then mapping (GeoJSON dict) for consistency
                            from shapely.geometry import mapping
                            geom_obj = wkb.loads(geom)
                            geom = mapping(geom_obj)
                        except Exception as e:
                            logger.warning(f"Address geometry parse error: {e}")
                            continue

                    addr = {
                        "id": row.get("id"),
                        "number": row.get("number"),
                        "street": row.get("street"),
                        "postcode": row.get("postcode"),
                        "geometry": geom,
                        "sources": row.get("sources", []),
                    }
                    addresses.append(addr)
                
                if len(addresses) >= limit:
                    break
                    
            logger.info(f"Found {len(addresses)} addresses")
            return addresses
            
        except Exception as e:
            logger.error(f"Error querying Overture addresses: {e}")
            return []

    def query_divisions(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Query Overture Maps administrative divisions.
        
        Args:
            bbox: Bounding box
            limit: Max results
            
        Returns:
            List of division features
        """
        if not self._ensure_initialized():
            return []

        try:
            logger.info(f"Querying Overture divisions with bbox={bbox}")
            record_iter = self._overture.record_batch_reader(
                "division", bbox=bbox
            )
            
            from shapely import wkb
            
            divisions = []
            for batch in record_iter:
                for row in batch.to_pylist():
                    if len(divisions) >= limit:
                        break
                    
                    geom = row.get("geometry")
                    if isinstance(geom, bytes):
                        try:
                            from shapely.geometry import mapping
                            geom_obj = wkb.loads(geom)
                            geom = mapping(geom_obj)
                        except Exception as e:
                            logger.warning(f"Division geometry parse error: {e}")
                            continue

                    div = {
                        "id": row.get("id"),
                        "names": row.get("names", {}),
                        "subtype": row.get("subtype"),
                        "country_iso": row.get("country_iso"),
                        "geometry": geom,
                        "validity_extent": row.get("validity_extent"),
                    }
                    divisions.append(div)
                
                if len(divisions) >= limit:
                    break
                    
            logger.info(f"Found {len(divisions)} divisions")
            return divisions
            
        except Exception as e:
            logger.error(f"Error querying Overture divisions: {e}")
            return []

    def query_transportation_near_point(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 2.0,
        road_class_filter: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Query transportation data near a specific point.

        Args:
            latitude: Center latitude
            longitude: Center longitude
            radius_km: Search radius in kilometers
            road_class_filter: Optional road class filter

        Returns:
            Dict with 'segments' and 'connectors'
        """
        import math
        
        lat_delta = radius_km / 111.0
        lon_delta = radius_km / (111.0 * math.cos(math.radians(latitude)))

        bbox = (
            longitude - lon_delta,
            latitude - lat_delta,
            longitude + lon_delta,
            latitude + lat_delta,
        )

        return self.query_transportation(bbox=bbox, road_class_filter=road_class_filter)


# Singleton instance for convenience
_client: Optional[OvertureMapsClient] = None


def get_overture_client() -> OvertureMapsClient:
    """Get or create a singleton Overture Maps client."""
    global _client
    if _client is None:
        _client = OvertureMapsClient()
    return _client
