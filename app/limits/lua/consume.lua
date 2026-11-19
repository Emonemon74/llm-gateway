-- Atomic dual token bucket: requests/min (RPM) and tokens/min (TPM), all-or-nothing.
-- KEYS[1]=rpm bucket, KEYS[2]=tpm bucket
-- ARGV: rpm_cap, rpm_rate_per_ms, tpm_cap, tpm_rate_per_ms, tpm_cost
-- Returns {status, limited_by, retry_after_ms, rpm_remaining, tpm_remaining}
--   status: 1 allowed, 0 rate limited, -1 request can never fit (cost > tpm capacity)
-- Time comes from Redis (TIME) so every API replica shares one clock.
local function refilled(key, cap, rate, now)
  local d = redis.call('HMGET', key, 't', 'ts')
  local t, ts = tonumber(d[1]), tonumber(d[2])
  if t == nil then return cap end
  return math.min(cap, t + math.max(0, now - ts) * rate)
end

local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)

local rpm_cap, rpm_rate = tonumber(ARGV[1]), tonumber(ARGV[2])
local tpm_cap, tpm_rate = tonumber(ARGV[3]), tonumber(ARGV[4])
local cost = tonumber(ARGV[5])

local r = refilled(KEYS[1], rpm_cap, rpm_rate, now)
local p = refilled(KEYS[2], tpm_cap, tpm_rate, now)

if cost > tpm_cap then
  return {-1, 'tpm', 0, math.floor(r), math.floor(p)}
end
if r < 1 then
  return {0, 'rpm', math.ceil((1 - r) / rpm_rate), math.floor(r), math.floor(p)}
end
if p < cost then
  return {0, 'tpm', math.ceil((cost - p) / tpm_rate), math.floor(r), math.floor(p)}
end

r = r - 1
p = p - cost
local ttl = math.ceil(math.max(rpm_cap / rpm_rate, tpm_cap / tpm_rate)) + 1000
redis.call('HSET', KEYS[1], 't', r, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('HSET', KEYS[2], 't', p, 'ts', now)
redis.call('PEXPIRE', KEYS[2], ttl)
return {1, '', 0, math.floor(r), math.floor(p)}
