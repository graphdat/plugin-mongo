local boundary = require("boundary")
local os = require("os")
local timer = require("timer")
local http = require("http")
local json = require("json")
local fs = require("fs")

-- set default parameters if not found in param.json
local param = boundary.param or {
    source = os.hostname,
    pollInterval = 5000,
    hostname = "127.0.0.1",
    port = "28017"
}

-- lookup
-- use an array of attrs to drill down through nested tables to retrieve a value
function lookup(table, attrs)
    local v = table;
    for i, a in ipairs(attrs) do
        v = v[a]
    end
    return v
end

-- fnFactory returns a function(current, previous) that when called
-- will output the metric string ready for printing
--      name - name of metric
--      func - type of metric (diff, cur or ratio)
--      format - printf type format specifier for the result eg "%d"
--      params - parameters specific to build the required function
function fnFactory(name, func, format, params)
    local mask = "MONGO_" .. name .. " " .. format .. " %s\n"
    local str = function(v) return string.format(mask, v, param.source) end

    return ({
        -- diff calculates the difference between a current and previous values
        diff = function (attrs, scale)
            return function (c, p)
                return str((scale or 1) * lookup(c, attrs) - lookup(p, attrs))
            end
        end,
        -- cur returns a current value
        cur = function (attrs, scale)
            return function (c)
                return str((scale or 1) * lookup(c, attrs))
            end
        end,
        -- ratio takes values a and b from the current data and returns a/b
        ratio = function (attrs1, attrs2, scale)
            return function (c)
                return str((scale or 1) * lookup(c, attrs1) / lookup(c, attrs2))
            end
        end,
        -- split takes values a and b from the current data and returns a/(a+b)
        split = function (attrs1, attrs2, scale)
            return function (c)
                local a = lookup(c, attrs1)
                return str((scale or 1) * a / (a + lookup(c, attrs2)))
            end
        end
    })[func](unpack(params))
end

-- build a table of functions to convert metrics into strings
local conversions = {}
for i, v in ipairs({
    {"BTREE_ACCESSES", "diff", "%d", {{"indexCounters", "accesses"}}},
    {"BTREE_HITS", "diff", "%d", {{"indexCounters", "hits"}}},
    {"BTREE_MISSES", "diff", "%d", {{"indexCounters", "misses"}}},
    {"BTREE_RESETS", "diff", "%d", {{"indexCounters", "resets"}}},
    {"BTREE_MISS_RATIO", "diff", "%d", {{"indexCounters", "missRatio"}}},
    {"CONNECTIONS", "cur", "%d", {{"connections", "current"}}},
    {"CONNECTIONS_AVAILABLE", "cur", "%d", {{"connections", "available"}}},
    {"CONNECTION_LIMIT", "split", "%f", {{"connections", "current"}, {"connections", "available"}}},
    {"GLOBAL_LOCK", "ratio", "%f", {{"globalLock", "lockTime"}, {"globalLock", "totalTime"}}},
    {"MEM_RESIDENT", "cur", "%d", {{"mem", "resident"}, 1024*1024}},
    {"MEM_VIRTUAL", "cur", "%d", {{"mem", "virtual"}, 1024*1024}},
    {"MEM_MAPPED", "cur", "%d", {{"mem", "mapped"}, 1024*1024}},
    {"OPS_INSERTS", "diff", "%d", {{"opcounters", "insert"}}},
    {"OPS_QUERY", "diff", "%d", {{"opcounters", "query"}}},
    {"OPS_UPDATE", "diff", "%d", {{"opcounters", "update"}}},
    {"OPS_DELETE", "diff", "%d", {{"opcounters", "delete"}}},
    {"OPS_GETMORE", "diff", "%d", {{"opcounters", "getmore"}}},
    {"OPS_COMMAND", "diff", "%d", {{"opcounters", "command"}}}
}) do
    table.insert(conversions, fnFactory(table.unpack(v)))
end

print("_bevent:Boundary MongoDB plugin up : version 1.0|t:info|tags:lua,mongodb,plugin")

local previous;

-- poll the server every pollInterval and use the 
-- conversion functions to extract relevent data
timer.setInterval(param.pollInterval, function ()
    local data = ""
    local req = http.request({
        host = param.hostname,
        port = param.port,
        path = "/_status"
    }, function (res)
        res:on("end", function ()
            current = json.parse(data).serverStatus
            if (previous) then
                local t = {}
                for i, f in ipairs(conversions) do
                    table.insert(t, f(current, previous))
                end
                fs.writeSync(1, -1, table.concat(t))
            end
            previous = current
            res:destroy()
        end)
        res:on("data", function (chunk) data = data .. chunk end)
        res:on("error", function (err) end)
    end)
    req:on("error", function(err)
        msg = tostring(err)
        print("Error while sending a request: " .. msg)
    end)

    req:done()
end)

