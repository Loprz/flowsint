import { useQuery } from '@tanstack/react-query'
import { useParams } from '@tanstack/react-router'
import { useGraphStore } from '@/stores/graph-store'
import { MapFromAddress } from './map'
import { LocationPoint, RouteData } from './map'

const MapPanel = () => {
  const nodes = useGraphStore((state) => state.nodes)
  const selectedNodes = useGraphStore((state) => state.selectedNodes)
  // Use strict: false to bypass type checking for params if route tree isn't fully typed here
  const params = useParams({ strict: false })
  const sketchId = params?.id as string

  // Logic to show all nodes if none selected, or only selected + related?
  // For now, let's keep showing all nodes, but highlight the route if 2 selected

  const locationNodes = nodes
    .filter((node) => node.nodeType === 'location' || node.nodeType === 'place' || node.nodeType === 'building' || (node.nodeProperties.latitude && node.nodeProperties.longitude))
    .map((node) => ({
      lat: node.nodeProperties.latitude || 0,
      lon: node.nodeProperties.longitude || 0,
      address: node.nodeProperties.address || node.nodeProperties.name || '',
      label: node.nodeProperties.label || node.nodeProperties.name || '',
      isOrigin: selectedNodes.length === 2 && node.nodeId === selectedNodes[0].nodeId,
      isDestination: selectedNodes.length === 2 && node.nodeId === selectedNodes[1].nodeId,
      geometry: node.nodeProperties.geometry,  // WKT geometry for polygon rendering
      nodeType: node.nodeType  // building, place, location, etc.
    }))

  const routeQuery = useQuery({
    queryKey: ['route', sketchId, selectedNodes.map((n) => n.nodeId)],
    queryFn: async () => {
      if (selectedNodes.length !== 2) return null

      const res = await fetch('/api/routing/shortest-path', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sketch_id: sketchId,
          origin_node_id: selectedNodes[0].nodeId,
          destination_node_id: selectedNodes[1].nodeId,
          algorithm: 'dijkstra'
        })
      })

      if (!res.ok) throw new Error('Failed to fetch route')
      return res.json()
    },
    enabled: !!sketchId && selectedNodes.length === 2,
    retry: false
  })

  let route: RouteData | undefined
  if (routeQuery.data && routeQuery.data.success) {
    route = {
      coordinates: routeQuery.data.route,
      distanceM: routeQuery.data.distance_m,
      color: '#3b82f6' // blue color for route
    }
  }

  return (
    <div className="w-full grow h-full relative">
      <MapFromAddress
        locations={locationNodes as LocationPoint[]}
        route={route}
      />
      {routeQuery.isLoading && (
        <div className="absolute top-4 right-4 bg-background/80 backdrop-blur p-2 rounded shadow text-xs">
          Calculating route...
        </div>
      )}
      {routeQuery.isError && (
        <div className="absolute top-4 right-4 bg-destructive/10 text-destructive p-2 rounded shadow text-xs">
          No route found
        </div>
      )}
    </div>
  )
}

export default MapPanel
