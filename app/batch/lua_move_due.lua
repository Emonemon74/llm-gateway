-- Atomically move due retries from the delayed ZSET back onto the work stream.
-- KEYS[1]=delayed zset, KEYS[2]=stream; ARGV[1]=max items. Uses Redis TIME (one clock for all).
-- ZREM and XADD happen in one script, so an item is never lost or enqueued twice.
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('XADD', KEYS[2], '*', 'item', id)
end
return #ids
