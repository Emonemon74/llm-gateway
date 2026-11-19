-- Schedule an item for later: KEYS[1]=delayed zset; ARGV[1]=item id, ARGV[2]=delay ms.
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
return redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
