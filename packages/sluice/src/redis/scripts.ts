/**
 * Lua for the shared token pool. One pool = one hash tag, so every key of a
 * pool lives on one Redis Cluster slot and each script is atomic.
 *
 * KEYS[1] = key prefix "sluice:{<pool>}:"
 * ARGV[1] = pool name (used in pub/sub payloads), ARGV[2] = instance id.
 *
 * Invariant kept by every script:  free + sum(held) == cap + debt   (debt = tokens still out above a lowered cap)
 */
export const PROTOCOL_VERSION = 1;

const COMMON = `
local P = KEYS[1]
local POOL = ARGV[1]
local ME = ARGV[2]
local function k(s) return P .. s end
local t = redis.call('TIME')
local NOW = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function num(v) return tonumber(v) or 0 end
local function meta(f) return num(redis.call('HGET', k('meta'), f)) end
local function hnum(h, id) return num(redis.call('HGET', k(h), id)) end

local function return_tokens(n)
  if n <= 0 then return end
  local pay = math.min(n, meta('debt'))
  if pay > 0 then redis.call('HINCRBY', k('meta'), 'debt', -pay) end
  if n - pay > 0 then redis.call('HINCRBY', k('meta'), 'free', n - pay) end
end

-- Take n tokens for an instance that already uses them (re-registration): from free, the rest as debt.
local function take(n)
  if n <= 0 then return end
  local f = math.min(n, meta('free'))
  if f > 0 then redis.call('HINCRBY', k('meta'), 'free', -f) end
  if n - f > 0 then redis.call('HINCRBY', k('meta'), 'debt', n - f) end
end

-- 'gseq' (per-instance grant counter) survives forget() on purpose: it only ever increases, so a
-- late grant message from an earlier registration can always be recognised as stale.
local FIELDS = {'held','want','backlog','weight','caller','rank','since','seq','res','drift','dgs','dmiss'}
local function forget(id)
  local held = hnum('held', id)
  for _, h in ipairs(FIELDS) do redis.call('HDEL', k(h), id) end
  redis.call('ZREM', k('waiters'), id)
  redis.call('ZREM', k('lease'), id)
  return held
end

local function reclaim()
  local dead = redis.call('ZRANGEBYSCORE', k('lease'), '-inf', NOW)
  for _, id in ipairs(dead) do
    return_tokens(forget(id))
    redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'reclaim', 'instance', id)
  end
  return #dead
end

-- Weighted shares of cap by weight * backlog, largest-remainder rounding (shares sum to cap).
local function fair_shares()
  local cap = meta('cap')
  local ids = redis.call('ZRANGE', k('lease'), 0, -1)
  local total, w, s, rest = 0, {}, {}, {}
  for _, id in ipairs(ids) do
    local b = hnum('backlog', id)
    if b == 0 and hnum('held', id) > 0 then b = 1 end
    local wt = tonumber(redis.call('HGET', k('weight'), id)) or 1
    w[id] = wt * b
    total = total + w[id]
  end
  if total == 0 or cap <= 0 then
    for _, id in ipairs(ids) do s[id] = 0 end
    return s, #ids
  end
  local given = 0
  for _, id in ipairs(ids) do
    local exact = cap * w[id] / total
    s[id] = math.floor(exact)
    given = given + s[id]
    table.insert(rest, { id, exact - s[id] })
  end
  -- Ties broken by id: table.sort is not stable, and every node must agree on shares.
  table.sort(rest, function(a, b) return a[2] > b[2] or (a[2] == b[2] and a[1] < b[1]) end)
  local i = 1
  while given < cap and i <= #rest do
    s[rest[i][1]] = s[rest[i][1]] + 1; given = given + 1; i = i + 1
  end
  local active = 0
  for _, id in ipairs(ids) do if w[id] > 0 then active = active + 1 end end
  if cap >= active then
    for _, id in ipairs(ids) do
      if w[id] > 0 and s[id] == 0 then
        local maxId = id
        for _, other in ipairs(ids) do if s[other] > s[maxId] then maxId = other end end
        s[maxId] = s[maxId] - 1; s[id] = 1
      end
    end
  end
  return s, #ids
end

-- Hand free tokens to waiters: first those below their fair share, then anyone,
-- one token per waiter per round. Grants for other instances are pushed over pub/sub;
-- the caller's own grant is returned. Grants count as held immediately, so a lost
-- message is reconciled by renew's drift correction.
local function distribute(me)
  local free = meta('free')
  if free <= 0 then return 0 end
  local ws = redis.call('ZRANGE', k('waiters'), 0, -1)
  if #ws == 0 then return 0 end
  local shares = fair_shares()
  local want, held, grant = {}, {}, {}
  for _, id in ipairs(ws) do
    want[id] = hnum('want', id); held[id] = hnum('held', id); grant[id] = 0
  end
  for pass = 1, 2 do
    local progress = true
    while free > 0 and progress do
      progress = false
      for _, id in ipairs(ws) do
        if free == 0 then break end
        if want[id] > 0 and (pass == 2 or held[id] < (shares[id] or 0)) then
          free = free - 1; want[id] = want[id] - 1; held[id] = held[id] + 1
          grant[id] = grant[id] + 1; progress = true
        end
      end
    end
  end
  redis.call('HSET', k('meta'), 'free', free)
  local mine, mineg = 0, 0
  for _, id in ipairs(ws) do
    if grant[id] > 0 then
      redis.call('HSET', k('held'), id, held[id])
      redis.call('HSET', k('want'), id, want[id])
      if want[id] <= 0 then redis.call('ZREM', k('waiters'), id) end
      -- Every grant gets a per-instance sequence number, so the client can report exactly
      -- which grants it has seen and renew never "corrects" a grant still in transit.
      local g = redis.call('HINCRBY', k('gseq'), id, 1)
      if id == me then mine, mineg = grant[id], g
      else redis.call('PUBLISH', 'sluice:i:' .. id, 'g ' .. POOL .. ' ' .. grant[id] .. ' ' .. g) end
    end
  end
  if redis.call('ZCARD', k('waiters')) == 0 then redis.call('PUBLISH', k('ev'), 'cleared') end
  return mine, mineg
end

local function starved(me, shares)
  for _, id in ipairs(redis.call('ZRANGE', k('waiters'), 0, -1)) do
    if id ~= me and hnum('want', id) > 0 and hnum('held', id) < (shares[id] or 0) then return true end
  end
  return false
end

local function state(me, extra)
  local shares, n = fair_shares()
  local s = {
    cap = meta('cap'), free = meta('free'), debt = meta('debt'), epoch = meta('epoch'),
    inc = redis.call('HGET', k('meta'), 'inc'), v = meta('v'),
    contended = starved(me, shares),
    share = shares[me] or 0, instances = n, held = hnum('held', me), gall = hnum('gseq', me),
    fence = meta('fence'), owner = redis.call('HGET', k('meta'), 'owner') or false,
    total = meta('ltotal'),
  }
  if extra then for key, val in pairs(extra) do s[key] = val end end
  return cjson.encode(s)
end

local function registered(id)
  return redis.call('EXISTS', k('meta')) == 1 and redis.call('ZSCORE', k('lease'), id) ~= false
end
`;

/** ARGV: pool, id, caller, weight, initCap, leaseMs, incarnation, heldNow, protocol */
export const JOIN = `${COMMON}
reclaim()
local v = tonumber(ARGV[9])
if redis.call('EXISTS', k('meta')) == 0 then
  local cap = tonumber(ARGV[5])
  redis.call('HSET', k('meta'), 'cap', cap, 'free', cap, 'debt', 0, 'epoch', 1, 'inc', ARGV[7], 'v', v, 'fence', 0)
  redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'pool:create', 'instance', ME, 'cap', cap)
elseif meta('v') > v + 0 and meta('v') - v > 1 then
  return cjson.encode({ err = 'PROTOCOL', v = meta('v') })
end
-- A process restarting under the same id: its old tokens are gone with it.
return_tokens(forget(ME))
redis.call('ZADD', k('lease'), NOW + tonumber(ARGV[6]), ME)
redis.call('HSET', k('caller'), ME, ARGV[3])
redis.call('HSET', k('weight'), ME, ARGV[4])
local heldNow = tonumber(ARGV[8])
redis.call('HSET', k('held'), ME, heldNow)
take(heldNow)
redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'join', 'instance', ME, 'caller', ARGV[3])
return state(ME)
`;

/** ARGV: pool, id, seq, release, want, rank, backlog, leaseMs, heldKnown */
export const SYNC = `${COMMON}
reclaim()
if not registered(ME) then return cjson.encode({ err = 'NOPOOL' }) end
local seq = tonumber(ARGV[3])
local last = tonumber(redis.call('HGET', k('seq'), ME))
if last and seq <= last then return redis.call('HGET', k('res'), ME) end
redis.call('ZADD', k('lease'), NOW + tonumber(ARGV[8]), ME)
local release = tonumber(ARGV[4])
if release > 0 then
  local h = hnum('held', ME)
  local r = math.min(release, h)
  redis.call('HSET', k('held'), ME, h - r)
  return_tokens(r)
end
-- Grants already pushed but not yet seen by the client still count towards what it wants.
local inTransit = math.max(0, hnum('held', ME) - tonumber(ARGV[9]))
local want = math.max(0, tonumber(ARGV[5]) - inTransit)
local rank = tonumber(ARGV[6])
redis.call('HSET', k('want'), ME, want)
redis.call('HSET', k('backlog'), ME, tonumber(ARGV[7]))
redis.call('HSET', k('rank'), ME, rank)
if want > 0 then
  local since = tonumber(redis.call('HGET', k('since'), ME))
  if redis.call('ZSCORE', k('waiters'), ME) == false or not since then
    since = NOW
    redis.call('HSET', k('since'), ME, since)
    if redis.call('ZCARD', k('waiters')) == 0 then redis.call('PUBLISH', k('ev'), 'contended') end
  end
  redis.call('ZADD', k('waiters'), rank * 1e13 + since, ME)
else
  redis.call('ZREM', k('waiters'), ME)
  redis.call('HDEL', k('since'), ME)
end
local mine, mineg = distribute(ME)
local res = state(ME, { granted = mine, gseq = mineg, seq = seq })
redis.call('HSET', k('seq'), ME, seq)
redis.call('HSET', k('res'), ME, res)
return res
`;

/** ARGV: pool, id, leaseMs, heldLocal (incl. unsent releases), backlog, ownerTtlMs (0 = not a candidate), statsJson, appliedGrantSeq */
export const RENEW = `${COMMON}
reclaim()
if not registered(ME) then return cjson.encode({ err = 'NOPOOL' }) end
redis.call('ZADD', k('lease'), NOW + tonumber(ARGV[3]), ME)
redis.call('HSET', k('backlog'), ME, tonumber(ARGV[5]))
-- Drift correction. ARGV[4] is what the client holds (including releases not yet sent) and
-- ARGV[8] is the highest grant number it has applied without gaps. A mismatch is only trusted
-- once the client has seen every grant we sent (or a grant has been missing for 3 renews in a
-- row, i.e. its message was lost), and only if it persists for two renews.
local sh = hnum('held', ME)
local cl = tonumber(ARGV[4])
local sg = hnum('gseq', ME)
local cg = tonumber(ARGV[8]) or 0
local settled = cg >= sg
if not settled then
  if tonumber(redis.call('HGET', k('dgs'), ME)) == sg then
    if redis.call('HINCRBY', k('dmiss'), ME, 1) >= 3 then settled = true end
  else
    redis.call('HSET', k('dgs'), ME, sg)
    redis.call('HSET', k('dmiss'), ME, 0)
  end
else
  redis.call('HSET', k('dmiss'), ME, 0)
end
local drift = 0
local gack = false
if settled and sh ~= cl then
  if redis.call('HINCRBY', k('drift'), ME, 1) >= 2 then
    if sh > cl then return_tokens(sh - cl) else take(cl - sh) end
    redis.call('HSET', k('held'), ME, cl)
    redis.call('HSET', k('drift'), ME, 0)
    redis.call('HSET', k('dmiss'), ME, 0)
    drift = sh - cl
    -- The client must now ignore any grant numbered up to sg that it has not seen yet.
    gack = sg
    redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'drift', 'instance', ME, 'delta', drift)
  end
else
  redis.call('HSET', k('drift'), ME, 0)
end
local ttl = tonumber(ARGV[6])
if ttl > 0 then
  local owner = redis.call('HGET', k('meta'), 'owner')
  local oexp = meta('oexp')
  if owner == false or owner == ME or oexp < NOW then
    if owner ~= ME then redis.call('HINCRBY', k('meta'), 'fence', 1) end
    redis.call('HSET', k('meta'), 'owner', ME, 'oexp', NOW + ttl)
  end
end
if ARGV[7] ~= '' then
  redis.call('SET', k('stats:' .. ME), ARGV[7], 'PX', tonumber(ARGV[3]) * 2)
end
local mine, mineg = distribute(ME)
return state(ME, { granted = mine, gseq = mineg, drift = drift, gack = gack })
`;

/**
 * ARGV: pool, id, capacity, fence, mode, total.
 * - fence = "admin" is an operator override: it wins over the cap-owner until
 *   cleared (capacity = "clear").
 * - mode = "shard": the logical owner of a sharded pool sets this shard's cap. The
 *   fence is checked against the shard's logical fence ('lfence'); "total" is the
 *   pool-wide N. With mode = "shard-free" a decrease only takes free tokens, so
 *   moving capacity between shards never creates debt.
 */
export const SETCAP = `${COMMON}
reclaim()
local mode = ARGV[5] or ''
local sharded = mode == 'shard' or mode == 'shard-free'
if redis.call('EXISTS', k('meta')) == 0 then
  if not sharded then return cjson.encode({ err = 'NOPOOL' }) end
  redis.call('HSET', k('meta'), 'cap', 0, 'free', 0, 'debt', 0, 'epoch', 1, 'inc', 'shard', 'v', 1, 'fence', 0, 'lfence', 0)
end
local admin = ARGV[4] == 'admin'
if admin and ARGV[3] == 'clear' then
  redis.call('HDEL', k('meta'), 'override')
  redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'override:clear', 'instance', ME)
  return state(ME)
end
if sharded then
  if tonumber(ARGV[4]) < meta('lfence') then return cjson.encode({ err = 'STALE_FENCE', fence = meta('lfence') }) end
  redis.call('HSET', k('meta'), 'lfence', ARGV[4], 'ltotal', tonumber(ARGV[6]) or 0)
elseif not admin then
  if redis.call('HEXISTS', k('meta'), 'override') == 1 then return cjson.encode({ err = 'OVERRIDDEN' }) end
  if tonumber(ARGV[4]) < meta('fence') then return cjson.encode({ err = 'STALE_FENCE', fence = meta('fence') }) end
else
  redis.call('HSET', k('meta'), 'override', ARGV[3])
end
local n = math.max(0, math.floor(tonumber(ARGV[3])))
local cap = meta('cap')
if mode == 'shard-free' and n < cap then n = math.max(n, cap - meta('free')) end
local delta = n - cap
if delta == 0 then return state(ME) end
redis.call('HSET', k('meta'), 'cap', n)
if delta > 0 then
  local cancel = math.min(delta, meta('debt'))
  redis.call('HINCRBY', k('meta'), 'debt', -cancel)
  redis.call('HINCRBY', k('meta'), 'free', delta - cancel)
else
  local fromFree = math.min(-delta, meta('free'))
  redis.call('HINCRBY', k('meta'), 'free', -fromFree)
  redis.call('HINCRBY', k('meta'), 'debt', -delta - fromFree)
end
redis.call('PUBLISH', k('ev'), 'capacity ' .. n)
redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'capacity', 'instance', ME, 'from', cap, 'to', n)
local mine, mineg = distribute(ME)
return state(ME, { granted = mine, gseq = mineg })
`;

/** ARGV: pool, id */
export const LEAVE = `${COMMON}
return_tokens(forget(ME))
redis.call('XADD', k('log'), 'MAXLEN', '~', 1000, '*', 'type', 'leave', 'instance', ME)
distribute(nil)
return 'OK'
`;

/** Read-only view for the control panel. ARGV: pool, '' */
export const INSPECT = `${COMMON}
if redis.call('EXISTS', k('meta')) == 0 then return cjson.encode({ err = 'NOPOOL' }) end
local ids = redis.call('ZRANGE', k('lease'), 0, -1, 'WITHSCORES')
local shares = fair_shares()
local instances = {}
for i = 1, #ids, 2 do
  local id = ids[i]
  table.insert(instances, {
    id = id, leaseMs = tonumber(ids[i + 1]) - NOW, held = hnum('held', id), want = hnum('want', id),
    backlog = hnum('backlog', id), weight = tonumber(redis.call('HGET', k('weight'), id)) or 1,
    caller = redis.call('HGET', k('caller'), id) or '', share = shares[id] or 0,
    waiting = redis.call('ZSCORE', k('waiters'), id) ~= false,
    stats = redis.call('GET', k('stats:' .. id)) or false,
  })
end
return cjson.encode({
  pool = POOL, cap = meta('cap'), free = meta('free'), debt = meta('debt'), epoch = meta('epoch'),
  override = redis.call('HGET', k('meta'), 'override') or false,
  fence = meta('fence'), owner = redis.call('HGET', k('meta'), 'owner') or false,
  waiters = redis.call('ZCARD', k('waiters')), instances = instances, now = NOW,
})
`;

/**
 * Claim (or renew) the logical capacity owner of a sharded pool on shard 0.
 * ARGV: pool, id, ttlMs. Returns { owner, fence }.
 */
export const CLAIM = `${COMMON}
if redis.call('EXISTS', k('meta')) == 0 then
  redis.call('HSET', k('meta'), 'cap', 0, 'free', 0, 'debt', 0, 'epoch', 1, 'inc', 'shard', 'v', 1, 'fence', 0, 'lfence', 0)
end
local owner = redis.call('HGET', k('meta'), 'lowner')
if owner == false or owner == ME or meta('loexp') < NOW then
  if owner ~= ME then redis.call('HINCRBY', k('meta'), 'lfence_next', 1) end
  redis.call('HSET', k('meta'), 'lowner', ME, 'loexp', NOW + tonumber(ARGV[3]))
  owner = ME
end
return cjson.encode({ owner = owner, fence = meta('lfence_next') })
`;

/**
 * Delete an abandoned pool: only when no instance is registered and no operator
 * override is pinned. ARGV: pool, "". Returns 1 if deleted.
 */
export const GC = `${COMMON}
reclaim()
if redis.call('ZCARD', k('lease')) > 0 then return 0 end
if redis.call('HEXISTS', k('meta'), 'override') == 1 then return 0 end
for _, h in ipairs({'meta','held','want','backlog','weight','caller','rank','since','seq','res','drift','waiters','lease','log'}) do
  redis.call('DEL', k(h))
end
return 1
`;
