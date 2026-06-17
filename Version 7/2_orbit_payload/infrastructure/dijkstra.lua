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
local mig_type   = ARGV[1]   -- "thermal" / "energy" → joint health, "lateral" → orbit plane preference
local T_safe     = tonumber(ARGV[2])
local T_fuse     = tonumber(ARGV[3])

-- Read dynamic battery thresholds or handle backwards compatibility if not passed
local B_safe     = 15.0
local B_fuse     = 5.0
local exclude_node = ""

if ARGV[4] then
    local possible_b_safe = tonumber(ARGV[4])
    if possible_b_safe then
        B_safe = possible_b_safe
        B_fuse = tonumber(ARGV[5]) or 5.0
        exclude_node = ARGV[6] or ""
    else
        exclude_node = ARGV[4]
    end
end

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
    -- Convert string planes ("A", "B", "C") to numerical indices (West to East)
    local plane_idx = { ["A"] = 1, ["B"] = 2, ["C"] = 3 }

    if mig_type == "lateral_east" or mig_type == "lateral_west" then
        local p_from = plane_idx[planes[from_name]]
        local p_to = plane_idx[planes[to_name]]

        if p_from and p_to then
            -- Calculate the directional jump (-1 means West, +1 means East)
            local diff = p_to - p_from

            -- Handle cyclic orbital wrap-around for a 3-plane constellation
            -- e.g., jumping East from Plane C (3) to Plane A (1) gives -2. Wrap it to +1.
            if diff == -2 then diff = 1 end
            -- e.g., jumping West from Plane A (1) to Plane C (3) gives 2. Wrap it to -1.
            if diff == 2 then diff = -1 end

            -- Apply massive discount (0.1) ONLY for the mathematically correct direction
            if mig_type == "lateral_east" and diff == 1 then
                w = w * 0.1
            elseif mig_type == "lateral_west" and diff == -1 then
                w = w * 0.1
            else
                -- Heavily penalize jumping backward or staying in the same plane
                w = w * 2.0
            end
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
-- STEP 5: Reconstruct the shortest path using Composite Scalarization.
-- Multi-Objective Optimization (MOO) balances Thermal, Energy, and Path costs.
-- -----------------------------------------------------------------------

local best_dest = nil
local best_score = INF

for _, name in ipairs(all_nodes) do
    -- Only evaluate nodes we can actually reach (dist < INF)
    if name ~= source and name ~= exclude_node and dist[name] < INF then

        local target_score

        if mig_type == "lateral_east" or mig_type == "lateral_west" then
            -- LATERAL LOGIC: Rely entirely on the directional path cost
            -- computed in Step 3 (which applies the 0.1 directional discount).
            target_score = dist[name]
        else
            -- JOINT MULTIPLICATIVE HEALTH INDEX (Thermal & Energy)
            -- Normalize within the critical threat ranges: [T_safe, T_fuse] and [B_fuse, B_safe]
            
            -- 1. Temperature Health: 1.0 (nominal) to 0.001 (critical limit reached)
            local t_penalty = math.min(1.0, math.max(0.0, (temps[name] - T_safe) / (T_fuse - T_safe)))
            local h_temp = math.max(0.001, 1.0 - t_penalty)

            -- 2. Battery Health: 1.0 (nominal) to 0.001 (critical limit reached)
            local b_penalty = math.min(1.0, math.max(0.0, (B_safe - batts[name]) / (B_safe - B_fuse)))
            local h_batt = math.max(0.001, 1.0 - b_penalty)

            -- 3. Minimizing the negative log-utility: S = -ln(H_T) - ln(H_B) + (dist * 0.1)
            -- Multi-Objective Optimization selection that avoids extreme weak links.
            target_score = -math.log(h_temp) - math.log(h_batt) + (dist[name] * 0.1)
        end

        -- Pick the node with the lowest overall penalty score
        if target_score < best_score then
            best_score = target_score
            best_dest = name
        end
    end
end

-- If no valid destination was found, return an error object
if not best_dest then
    return cjson.encode({ error = "NO_ROUTE", source = source })
end

-- Walk back through the predecessor map to build the complete multi-hop route
local route = {}
local current = best_dest
while current and current ~= source do
    table.insert(route, 1, current)  -- Prepend to get source->destination order
    current = prev[current]
end

-- -----------------------------------------------------------------------
-- STEP 6: Return the result as a JSON object.
-- The Node Agent will parse this on the Python side.
-- -----------------------------------------------------------------------
return cjson.encode({
    route = route,         -- e.g. ["minikube-m03", "minikube-m04"]
    type  = mig_type,      -- "thermal" or "lateral"
    cost  = dist[best_dest]      -- Total path cost (useful for telemetry/logging)
})
