-- Circuit breaker state machine, atomic, shared by all API replicas.
-- KEYS[1]=breaker hash. ARGV: action(allow|success|failure), threshold, cooldown_ms, probe_timeout_ms
-- States: closed -> (threshold consecutive failures) -> open -> (cooldown) -> half_open
--         half_open: exactly ONE probe request at a time; success -> closed, failure -> open.
-- Returns {allowed(1/0), state}
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
local d = redis.call('HMGET', KEYS[1], 'state', 'failures', 'opened_at', 'probe_at')
local state = d[1] or 'closed'
local failures = tonumber(d[2]) or 0
local opened_at = tonumber(d[3]) or 0
local probe_at = tonumber(d[4]) or 0
local action = ARGV[1]
local threshold, cooldown, probe_timeout = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local ttl = 3600000

if action == 'allow' then
  if state == 'closed' then return {1, 'closed'} end
  if state == 'open' then
    if now - opened_at >= cooldown then
      redis.call('HSET', KEYS[1], 'state', 'half_open', 'probe_at', now)
      redis.call('PEXPIRE', KEYS[1], ttl)
      return {1, 'half_open'}
    end
    return {0, 'open'}
  end
  -- half_open: a probe is already in flight unless its lease expired (probe crashed/hung)
  if now - probe_at >= probe_timeout then
    redis.call('HSET', KEYS[1], 'probe_at', now)
    return {1, 'half_open'}
  end
  return {0, 'half_open'}
end

if action == 'success' then
  if state == 'open' then return {0, 'open'} end -- late success from before it opened: ignore
  if state == 'half_open' or failures > 0 then
    redis.call('HSET', KEYS[1], 'state', 'closed', 'failures', 0)
    redis.call('PEXPIRE', KEYS[1], ttl)
  end
  return {1, 'closed'}
end

-- failure
if state == 'open' then return {0, 'open'} end
if state == 'half_open' then
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', now, 'failures', threshold)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {0, 'open'}
end
failures = failures + 1
if failures >= threshold then
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', now, 'failures', failures)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {0, 'open'}
end
redis.call('HSET', KEYS[1], 'state', 'closed', 'failures', failures)
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, 'closed'}
