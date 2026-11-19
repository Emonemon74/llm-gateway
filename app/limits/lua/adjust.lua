-- Reconcile the TPM bucket after the real token usage is known.
-- KEYS[1]=tpm bucket; ARGV: cap, rate_per_ms, delta (actual - estimated; negative = refund)
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
local cap, rate, delta = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local d = redis.call('HMGET', KEYS[1], 't', 'ts')
local t, ts = tonumber(d[1]), tonumber(d[2])
if t == nil then return 0 end -- bucket expired: nothing to reconcile
t = math.min(cap, t + math.max(0, now - ts) * rate)
t = math.max(0, math.min(cap, t - delta))
redis.call('HSET', KEYS[1], 't', t, 'ts', now)
return math.floor(t)
