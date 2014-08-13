// log the error as soon as possible
process.on('uncaughtException', function(err) {
    console.error('msg %s, name %s, stack->\n%s', err.message, err.name, err.stack || 'NONE');
    process.exit(-1);
});

var _format = require('util').format;
var _os = require('os');
var _param = require('./param.json');
var _request = require('request');

var _pollInterval; // the interval to poll the metrics
var _previous = {}; // remember the previous poll data so we can provide proper counts
var _source; // the source of the metrics

// ================
// HELPER FUNCTIONS
// ================

// get the natural difference between a and b
function diff(a, b)
{
    if (a == null || b == null)
        return 0;
    else
        return Math.max(a - b, 0);
}

// get the natural sum of the passed in values
function sum()
{
    var s = 0;
    for (var i = 0; i < arguments.length; i++)
        s += (arguments[i] == null || isNaN(arguments[i])) ? 0 : arguments[i];

    return Math.max(s, 0);
}

// if its number return it, otherwise return 0
function parse(x)
{
    var y = parseFloat(x, 10);
    return (isNaN(y) ? 0 : y);
}

// parse some JSON
function parseJSON(json)
{
    var obj;
    try
    {
        obj = JSON.parse(json);
    }
    catch(ex)
    {
        return '';
    }
    return obj;
}

// ==========
// VALIDATION
// ==========

// hostname of the mongo server
var _hostname = _param.hostname || '127.0.0.1';

// If you do not have a poll intevarl, use 1000 as the default
var _pollInterval = _param.pollInterval || 1000;

// the port of the mongo http service is +1000 of the mongo port.
// if the user knows this and sets the right port, continue,
// otherwise +1000 to the custom port,
// otherwise use the default 28107
var _port = (_param.port && _param.port != 28017) ? parse(_param.port) + 1000 : 28017;

// if we do not have a source, then set it
var _source = _param.source || _os.hostname();

// generate the URL
var _mongoStatsUrl = _format('http://%s:%d/_status', _hostname, _port);

// if we have a name and password, then add an auth header
var _httpOptions;
if (_param.username)
    _httpOptions = { user: _param.username, pass: _param.password, sendImmediately: true };

// call mongo and parse the stats
function getStats(cb)
{
    // call mongo to get the stats page
    _request.get(_mongoStatsUrl, _httpOptions, function(err, resp, body)
    {
        if (err)
            return cb(err);
        if (resp.statusCode !== 200)
            return cb(new Error('Mongo returned with an error - recheck the URL you provided'));
        if (!body)
            return cb(new Error('Mongo statistics returned empty'));

        var stats = parseJSON(body);
        if (!stats)
            return cb(new Error('Mongo statistics are invalid JSON'));

        return cb(null, stats.serverStatus || {});
    });
}

// get the stats, format the output and send to stdout
function poll(cb)
{
    getStats(function(err, current)
    {
        if (err)
            return console.error(err);

        /* BTREE STATS
            Accesses - reports the number of times that operations have accessed indexes.
                        This value is the combination of the hits and misses.
                        * Higher values indicate that your database has indexes and that queries
                        are taking advantage of these indexes. If this number does not grow over time,
                        this might indicate that your indexes do not effectively support your use.
            Hits - reports the number of times that an index has been accessed and mongod is
                        able to return the index from memory.
                        * A higher value indicates effective index use. hits values that represent a
                        greater proportion of the accesses value, tend to indicate more effective index configuration.
            Misses - reports the number of times that an operation attempted to access an index that was not in memory.
                        These “misses,” do not indicate a failed query or operation, but rather an inefficient use
                        of the index. Lower values in this field indicate better index use and likely overall
                        performance as well.
            Resets - reports the number of times that the index counters have been reset.
                        Typically this value is 0, but use this value to provide context for the data specified by other
                        indexCounters values.
            Miss ratio - The missRatio value is the ratio of hits to misses. This value is typically 0 or approaching 0.

            { accesses: 1, hits: 1, misses: 0, resets: 0, missRatio: 0 }
        */
        var p_btree = _previous && _previous.indexCounters && _previous.indexCounters.btree || _previous && _previous.indexCounters || {};
        var c_btree = current.indexCounters.btree || current.indexCounters;
        //console.log('MONGO_BTREE_ACCESSES %d %s', diff(c_btree.accesses, p_btree.accesses), _source);
        console.log('MONGO_BTREE_HITS %d %s', diff(c_btree.hits, p_btree.hits), _source);
        console.log('MONGO_BTREE_MISSES %d %s', diff(c_btree.misses, p_btree.misses), _source);
        //console.log('MONGO_BTREE_RESETS %d %s', diff(c_btree.resets, p_btree.resets), _source);
        console.log('MONGO_BTREE_MISS_RATIO %d %s', diff(c_btree.missRatio, p_btree.missRatio), _source); //percentage

        // Connection Stats
        var conn = current.connections.current;
        var availConn = current.connections.available;
        var usedRatio = current.connections.current / (conn + availConn);
        console.log('MONGO_CONNECTIONS %d %s', conn, _source);
        console.log('MONGO_CONNECTIONS_AVAILABLE %d %s', availConn, _source);
        console.log('MONGO_CONNECTION_LIMIT %d %s', usedRatio, _source);

        // Global lock ratio
        var gl_totalTime = current.globalLock.totalTime.$numberLong;
        var gl_lockTime = current.globalLock.lockTime.$numberLong;
        var global_lock = gl_lockTime / gl_totalTime;
        console.log('MONGO_GLOBAL_LOCK %d %s', global_lock, _source); //percentage

        /* MEMORY USAGE
            resident - value is roughly equivalent to the amount of RAM, in megabytes (MB),
                        currently used by the database process. In normal use this value tends to grow.
                        In dedicated database servers this number tends to approach the total amount of system memory.
            virtual - displays the quantity, in megabytes (MB), of virtual memory used by the mongod process.
                        With journaling enabled, the value of virtual is at least twice the value of mapped.
                        * If virtual value is significantly larger than mapped (e.g. 3 or more times),
                        this may indicate a memory leak.
            mapped - value provides the amount of mapped memory, in megabytes (MB), by the database.
                        Because MongoDB uses memory-mapped files, this value is likely to be to be roughly
                        equivalent to the total size of your database or databases.
            { bits: 64, resident: 18, virtual: 1359, supported: true, mapped: 160, mappedWithJournal: 320 }
        */
        var memory = current.mem;
        console.log('MONGO_MEM_RESIDENT %d %s', memory.resident*1024*1024, _source); //bytes
        console.log('MONGO_MEM_VIRTUAL %d %s', memory.virtual*1024*1024, _source); // bytes
        console.log('MONGO_MEM_MAPPED %d %s', memory.mapped*1024*1024, _source); //bytes

        /* OP COUNTERS
            insert - provides a counter of the total number of insert operations
            query - provides a counter of the total number of queries
            update - provides a counter of the total number of update operations
            delete - provides a counter of the total number of delete operations
            getmore - provides a counter of the total number of “getmore” operations.
                    This counter can be high even if the query count is low.
                    Secondary nodes send getMore operations as part of the replication process.
            command - provides a counter of the total number of commands issued to the database

            { insert: 519, query: 2750633, update: 2750449, delete: 10, getmore: 0, command: 449 }
        */
        var p_ops = _previous && _previous.opcounters || {};
        var c_ops = current.opcounters;
        console.log('MONGO_OPS_INSERTS %d %s', diff(c_ops.insert, p_ops.insert), _source);
        console.log('MONGO_OPS_QUERY %d %s', diff(c_ops.query, p_ops.query), _source);
        console.log('MONGO_OPS_UPDATE %d %s', diff(c_ops.update, p_ops.update), _source);
        console.log('MONGO_OPS_DELETE %d %s', diff(c_ops.delete, p_ops.delete), _source);
        console.log('MONGO_OPS_GETMORE %d %s', diff(c_ops.getmore, p_ops.getmore), _source);
        console.log('MONGO_OPS_COMMAND %d %s', diff(c_ops.command, p_ops.command), _source);

        _previous = current;
    });

    setTimeout(poll, _pollInterval);
}

poll();
