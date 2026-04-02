-- ==============================================================================
-- SPACE CLOUD V6 - ATOMIC DIJKSTRA PATHFINDER (Floating Master Lua Script)
-- Role: Runs entirely inside the Redis event loop to find the optimal escape
--       route through the ISL mesh. Because Lua scripts are atomic in Redis,
--       no telemetry update can race against this query mid-execution.
--
-- ARGV[1] = migration_type: "thermal" or "lateral"
-- ARGV[2] = T_safe: hardware temperature below which a node is considered healthy
-- ARGV[3] = T_fuse: hardware temperature above which a node is excluded from routing
-- KEYS[1] = source node name (e.g. "minikube-m02")
-- ==============================================================================

local source     = KEYS[1]
local mig_type   = ARGV[1]   -- "thermal" → find coolest trailing satellite
                              -- "lateral" → prefer adjacent parallel orbital plane
local T_safe     = tonumber(ARGV[2])
local T_fuse     = tonumber(ARGV[3])

-- -----------------------------------------------------------------------
-- STEP 1: Load all node telemetry from the Floating Master's Redis Hashes.
-- Each satellite pushes its own data: HSET node:<name> temp <T> battery <B>
--
-- Issue #3 fix: Replaced 'KEYS node:*' with 'SMEMBERS active_fleet'.
-- KEYS is O(N) against the ENTIRE Redis keyspace and BLOCKS the single-threaded
-- event loop during execution. With SMEMBERS, we read only from a dedicated Set
-- that each Node Agent populates via SADD. This is O(M) where M = fleet size.
-- -----------------------------------------------------------------------
local fleet_members = redis.call('SMEMBERS', 'active_fleet')

-- Tables that will hold each node's physics data
local temps    = {}  -- Hardware temperature per node
local batts    = {}  -- Battery level per node (0-100)
local planes   = {}  -- Orbital plane identifier per node (for lateral routing)
local all_nodes = {} -- Complete list of node names

for _, name in ipairs(fleet_members) do
    table.insert(all_nodes, name)

    -- Read the Hash fields for this node (key format: node:<name>)
    local t = redis.call('HGET', 'node:' .. name, 'temp')
    local b = redis.call('HGET', 'node:' .. name, 'battery')
    local p = redis.call('HGET', 'node:' .. name, 'orbit_plane')   -- "A", "B", "C", ...

    temps[name]  = tonumber(t) or 999
    batts[name]  = tonumber(b) or 0
    planes[name] = p or "A"
end

-- -----------------------------------------------------------------------
-- STEP 2: Load the adjacency graph from the "adj:<node>" Redis Sets.
-- Each Node Agent pushes SADD adj:<name> <neighbor> independently.
-- -----------------------------------------------------------------------
local adj = {}  -- adj[node] = { neighbor1, neighbor2, ... }

for _, name in ipairs(all_nodes) do
    adj[name] = redis.call('SMEMBERS', 'adj:' .. name)
end

-- -----------------------------------------------------------------------
-- STEP 3: Build the edge weights using the composite ISL cost function.
--
-- Standard weight (thermal mode):
--   w(u,v) = 1/SNR + (1 - B_v/100) + heat_penalty_v
--
-- Lateral mode applies an additional multiplier:
--   - Same orbital plane -> cost x1.5   (discourage trailing satellites)
--   - Different orbital plane -> cost x0.5 (encourage lateral neighbours)
-- -----------------------------------------------------------------------
local function edge_weight(from_name, to_name)
    local T_v = temps[to_name]
    local B_v = batts[to_name]

    -- Exclude nodes that are critically overheating
    if T_v >= T_fuse then
        return math.huge   -- Infinite cost = node is impassable
    end

    -- Heat penalty: 0 when cool, rises to 1 when approaching fuse temperature
    local heat_penalty = 0
    if T_v > T_safe then
        heat_penalty = (T_v - T_safe) / (T_fuse - T_safe)
    end

    -- Battery term: low battery means higher cost to route through that node
    local battery_cost = 1.0 - (B_v / 100.0)

    -- Base ISL cost (simplified SNR proxy: nodes with more battery have better radio power)
    local snr_cost = 1.0 / math.max(B_v / 100.0, 0.01)

    local w = snr_cost + battery_cost + heat_penalty

    -- Apply lateral routing bias: prefer different orbital planes for Trigger B migrations
    if mig_type == "lateral" then
        if planes[from_name] == planes[to_name] then
            w = w * 1.5   -- Same plane → discourage (this is a "trailing" satellite)
        else
            w = w * 0.5   -- Different plane → strongly prefer (lateral neighbor)
        end
    end

    return w
end

-- -----------------------------------------------------------------------
-- STEP 4: Dijkstra shortest-path algorithm (O(V^2 + E)).
-- Standard implementation using a dist table and a visited set.
-- -----------------------------------------------------------------------
local INF = math.huge

-- Initialize all distances to infinite, source to zero
local dist = {}
local prev = {}   -- Predecessor map to reconstruct the path
local visited = {}

for _, name in ipairs(all_nodes) do
    dist[name]    = INF
    prev[name]    = nil
    visited[name] = false
end

dist[source] = 0

-- Main loop: relax edges V times
for _ = 1, #all_nodes do
    -- Find the unvisited node with the smallest current distance
    local u = nil
    local min_d = INF
    for _, name in ipairs(all_nodes) do
        if not visited[name] and dist[name] < min_d then
            min_d = dist[name]
            u = name
        end
    end

    -- No reachable node found; graph may be disconnected
    if not u then break end
    visited[u] = true

    -- Relax all edges from u to its neighbours
    for _, v in ipairs(adj[u] or {}) do
        -- Skip the source node itself and nodes that are overheating
        if v ~= source and not visited[v] then
            local w = edge_weight(u, v)
            if w < INF then
                local new_dist = dist[u] + w
                if new_dist < dist[v] then
                    dist[v] = new_dist
                    prev[v] = u   -- Record the path predecessor
                end
            end
        end
    end
end

-- -----------------------------------------------------------------------
-- STEP 5: Reconstruct the shortest path from source to the best destination.
-- The best destination is the node with the lowest cost (excluding source).
-- -----------------------------------------------------------------------

-- Find the best reachable destination (lowest dist, excluding source and impassable nodes)
local best_dest = nil
local best_cost = INF

for _, name in ipairs(all_nodes) do
    if name ~= source and dist[name] < best_cost then
        best_cost = dist[name]
        best_dest = name
    end
end

-- If no valid destination was found, return an error object
if not best_dest then
    return cjson.encode({ error = "NO_ROUTE", source = source })
end

-- Walk back through the predecessor map to build the complete route
local route = {}
local current = best_dest
while current and current ~= source do
    table.insert(route, 1, current)  -- Prepend to get source→destination order
    current = prev[current]
end

-- -----------------------------------------------------------------------
-- STEP 6: Return the result as a JSON object.
-- The Node Agent will parse this on the Python side.
-- -----------------------------------------------------------------------
return cjson.encode({
    route = route,         -- e.g. ["minikube-m03", "minikube-m04"]
    type  = mig_type,      -- "thermal" or "lateral"
    cost  = best_cost      -- Total path cost (useful for telemetry/logging)
})
