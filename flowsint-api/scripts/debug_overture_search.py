
import asyncio
from tools.overture.client import get_overture_client
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_search():
    client = get_overture_client()
    
    # Coordinates for '13761 Ave 320' from logs
    lat = 36.3705601
    lon = -119.2661442
    
    print(f"Searching Overture near {lat}, {lon}...")
    
    # 1. Search Addresses (Radius 100m)
    print("\n--- ADDRESSES (100m) ---")
    try:
        # manual bbox for ~100m
        r = 0.001 
        bbox = (lon - r, lat - r, lon + r, lat + r)
        addresses = client.query_addresses(bbox=bbox, limit=10)
        for a in addresses:
            print(f"Found Address: {a.get('number')} {a.get('street')} (ID: {a.get('id')})")
            if a.get('geometry'):
                print(f"  Geom: {a.get('geometry')}")
        if not addresses:
            print("No addresses found.")
    except Exception as e:
        print(f"Error searching addresses: {e}")

    # 2. Search Buildings (Radius 100m)
    print("\n--- BUILDINGS (100m) ---")
    try:
        buildings = client.query_buildings(bbox=bbox, limit=10)
        for b in buildings:
            print(f"Found Building: {b.get('id')} (Class: {b.get('class')})")
        if not buildings:
            print("No buildings found.")
    except Exception as e:
        print(f"Error searching buildings: {e}")
        
    # 3. Search Streets (Radius 500m)
    print("\n--- STREETS (500m) ---")
    try:
        trans = client.query_transportation_near_point(lat, lon, radius_km=0.5)
        for s in trans.get('segments', []):
            print(f"Found Street: {s.get('name')} (ID: {s.get('id')})")
        if not trans.get('segments'):
            print("No streets found.")
    except Exception as e:
        print(f"Error searching streets: {e}")

if __name__ == "__main__":
    asyncio.run(debug_search())
