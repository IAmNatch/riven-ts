import type { Queue } from "bullmq";

type RedisClient = Awaited<Queue["client"]>;

/**
 * Result of recording an upstream HTTP 429 against the circuit breaker.
 */
interface RecordRateLimitResult {
  /**
   * The number of consecutive upstream 429s seen across ALL jobs on the queue
   * (reset to 0 by any successful response). Used to scale the no-`Retry-After`
   * backoff even when the breaker itself is disabled.
   */
  consecutive429s: number;
  /**
   * Whether this 429 tripped the breaker (moved it from closed to open).
   * Mutually exclusive with {@link alreadyOpen}.
   */
  tripped: boolean;
  /**
   * Whether this 429 landed while the breaker was ALREADY open — i.e. a
   * "straggler": a request that was in flight past the worker gate when an
   * earlier 429 tripped the breaker. Such 429s are deliberately NOT counted
   * toward a fresh trip and do NOT escalate the level or extend the open key's
   * TTL; the caller should simply defer the job for the remaining cooldown.
   * This is what keeps "one trip = one level bump": escalation only ever
   * happens on the genuine half-open probe once the open key has expired.
   * Mutually exclusive with {@link tripped}.
   */
  alreadyOpen: boolean;
  /**
   * When {@link tripped}, the cooldown (in ms) the breaker opened for. `0`
   * otherwise.
   */
  cooldownMs: number;
  /**
   * The escalation level after this call (1-based once tripped; 0 while
   * closed and never previously escalated).
   */
  level: number;
}

/**
 * Result of recording a successful upstream response against the breaker.
 */
interface RecordSuccessResult {
  /**
   * The escalation level after this call. Drops to `0` once
   * {@link CIRCUIT_BREAKER_SUCCESS_STREAK_TO_RESET_LEVEL} consecutive successes
   * have been observed.
   */
  level: number;
  /**
   * Whether this success reset a previously-escalated breaker back to level 0
   * (i.e. the datasource has demonstrably recovered).
   */
  recovered: boolean;
}

export interface DatasourceCircuitBreakerConfig {
  /**
   * Async accessor for the ioredis client (BullMQ's `queue.client` promise).
   * Resolved lazily so the breaker shares the datasource's existing connection.
   */
  client: Promise<RedisClient>;
  /**
   * Key prefix, namespaced by the fetch queue id, e.g.
   * `bull:<queueId>:circuit-breaker`. All breaker state lives under this.
   */
  keyPrefix: string;
  /**
   * Consecutive-429 count that trips the breaker while it is *not* already
   * escalated (level 0). `<= 0` disables tripping entirely (the breaker still
   * counts 429s so the escalating backoff keeps working).
   */
  threshold: number;
  /**
   * Base cooldown, in ms, applied on the first trip. Doubles per escalation
   * level, capped at {@link maxCooldownMs}.
   */
  baseCooldownMs: number;
  /**
   * Ceiling for the escalating cooldown, in ms.
   */
  maxCooldownMs: number;
}

/**
 * Consecutive successful upstream responses required to consider the datasource
 * recovered and reset the escalation level back to 0. Exported so callers can
 * describe recovery in their own operator logs.
 */
export const CIRCUIT_BREAKER_SUCCESS_STREAK_TO_RESET_LEVEL = 10;

/**
 * While the breaker is already escalated (level > 0) it re-trips after this
 * many consecutive 429s. Kept deliberately at 1 so the half-open probe (the
 * first requests allowed through once a cooldown expires) re-opens the breaker
 * *immediately* at the next escalation level if the upstream is still banning
 * us — a recently-banned scraper should back off hard rather than waste another
 * full base-threshold streak of requests re-provoking the ban.
 *
 * This escalated threshold only ever applies to that half-open probe, i.e. a
 * 429 seen AFTER the open key has expired. 429s that arrive *while the breaker
 * is still open* (in-flight stragglers that passed the worker gate before the
 * trip) are short-circuited earlier in {@link RECORD_RATE_LIMIT_SCRIPT} and
 * never reach this threshold, so a single trip bumps the level by exactly one
 * regardless of how many stragglers land during the open window.
 */
const CIRCUIT_BREAKER_ESCALATED_TRIP_THRESHOLD = 1;

/**
 * TTL, in ms, for the persisted escalation level and success-streak keys. Long
 * enough that a reopen shortly after a cooldown re-trips at the next level, but
 * bounded so a datasource that goes quiet eventually forgets its history.
 */
const CIRCUIT_BREAKER_LEVEL_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * TTL, in ms, for the consecutive-429 counter. It is normally cleared by the
 * next success; this bound just prevents a long-stale partial streak from
 * lingering forever after activity stops.
 */
const CIRCUIT_BREAKER_COUNTER_TTL_MS = 60 * 60 * 1000;

/**
 * Atomically records a 429: increments the consecutive-429 counter, clears the
 * success streak, and — when enabled — trips the breaker if the counter reaches
 * the (base or escalated) threshold. Doing this in a single Lua script keeps it
 * race-free across the many jobs a worker processes concurrently, so a burst of
 * simultaneous 429s cannot slip past the threshold.
 *
 * If the breaker is ALREADY open (`PTTL(open) > 0`) the 429 is a straggler — a
 * request that was already in flight past the worker gate when an earlier 429
 * tripped the breaker. It is recorded as "already open" and short-circuits
 * BEFORE any counting or trip/escalation logic: the counter is left untouched
 * (it was zeroed on the trip), the level is unchanged, and the open key's TTL
 * is NOT reset or extended. This is what prevents stragglers from re-escalating
 * during a single cooldown — escalation can only happen on the genuine
 * half-open probe once the open key has expired, so one trip bumps the level by
 * exactly one.
 *
 * KEYS: [consecutive, open, level, success]
 * ARGV: [baseThreshold, escalatedThreshold, baseCooldownMs, maxCooldownMs,
 *        levelTtlMs, counterTtlMs, enabled]
 * Returns: {consecutive429s, tripped(1|0), cooldownMs, level, alreadyOpen(1|0)}
 */
const RECORD_RATE_LIMIT_SCRIPT = `
if redis.call('PTTL', KEYS[2]) > 0 then
  local level = tonumber(redis.call('GET', KEYS[3]) or '0')
  local consecutive = tonumber(redis.call('GET', KEYS[1]) or '0')
  return {consecutive, 0, 0, level, 1}
end

local consecutive = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[6]))
redis.call('DEL', KEYS[4])

if tonumber(ARGV[7]) == 0 then
  return {consecutive, 0, 0, 0, 0}
end

local level = tonumber(redis.call('GET', KEYS[3]) or '0')
local threshold
if level > 0 then
  threshold = tonumber(ARGV[2])
else
  threshold = tonumber(ARGV[1])
end

if consecutive < threshold then
  return {consecutive, 0, 0, level, 0}
end

local newLevel = level + 1
local exp = newLevel - 1
if exp > 30 then exp = 30 end
local cooldown = tonumber(ARGV[3]) * (2 ^ exp)
local maxCd = tonumber(ARGV[4])
if cooldown > maxCd then cooldown = maxCd end
cooldown = math.floor(cooldown)

redis.call('SET', KEYS[2], '1', 'PX', cooldown)
redis.call('SET', KEYS[3], newLevel, 'PX', tonumber(ARGV[5]))
redis.call('SET', KEYS[1], 0)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[6]))

return {consecutive, 1, cooldown, newLevel, 0}
`;

/**
 * Atomically records a success: clears the consecutive-429 counter, and — if
 * the breaker is escalated — increments the success streak, resetting the
 * escalation level once the streak reaches the recovery threshold.
 *
 * KEYS: [consecutive, level, success]
 * ARGV: [successStreakToReset, levelTtlMs]
 * Returns: {level, recovered(1|0)}
 */
const RECORD_SUCCESS_SCRIPT = `
redis.call('DEL', KEYS[1])

local level = tonumber(redis.call('GET', KEYS[2]) or '0')
if level == 0 then
  redis.call('DEL', KEYS[3])
  return {0, 0}
end

local streak = redis.call('INCR', KEYS[3])
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[2]))

if streak >= tonumber(ARGV[1]) then
  redis.call('DEL', KEYS[2])
  redis.call('DEL', KEYS[3])
  return {0, 1}
end

return {level, 0}
`;

/**
 * A Redis-persisted, per-fetch-queue circuit breaker that protects an upstream
 * from a self-sustaining 429 retry storm.
 *
 * The failure mode this exists for: with a large backfill backlog, per-item
 * rate-limit retries create a *queue-level metronome* — item fires, gets 429'd,
 * the queue freezes for the backoff, the next item fires the instant the freeze
 * lifts, gets 429'd, and so on. Some upstreams (e.g. torrentio) only lift a
 * per-IP ban after a genuinely *quiet* period, so the metronome refreshes the
 * ban indefinitely. Bounding retries per item does nothing when the item count
 * is unbounded; the aggregate request rate must be bounded at the datasource
 * level. That is what this breaker does.
 *
 * State is namespaced by the fetch queue id and lives entirely in Redis, so it
 * survives process restarts (a restart must NOT reset the quiet-period clock)
 * and auto-expires via TTL (a crash must NEVER strand a queue permanently
 * blocked).
 */
export class DatasourceCircuitBreaker {
  readonly #client: Promise<RedisClient>;
  readonly #consecutiveKey: string;
  readonly #openKey: string;
  readonly #levelKey: string;
  readonly #successKey: string;
  readonly #threshold: number;
  readonly #baseCooldownMs: number;
  readonly #maxCooldownMs: number;

  public constructor(config: DatasourceCircuitBreakerConfig) {
    this.#client = config.client;
    this.#consecutiveKey = `${config.keyPrefix}:consecutive-429`;
    this.#openKey = `${config.keyPrefix}:open`;
    this.#levelKey = `${config.keyPrefix}:level`;
    this.#successKey = `${config.keyPrefix}:success-streak`;
    this.#threshold = config.threshold;
    this.#baseCooldownMs = config.baseCooldownMs;
    this.#maxCooldownMs = config.maxCooldownMs;
  }

  /**
   * Whether tripping is enabled. When disabled the breaker never opens, but it
   * still counts consecutive 429s so the escalating no-`Retry-After` backoff
   * keeps scaling.
   */
  public get enabled(): boolean {
    return this.#threshold > 0;
  }

  /**
   * The remaining cooldown, in ms, if the breaker is open; `0` if closed. Read
   * from the PTTL of the open key so it is correct after a process restart.
   */
  public async remainingCooldownMs(): Promise<number> {
    if (!this.enabled) {
      return 0;
    }

    const client = await this.#client;
    const pttl = await client.pttl(this.#openKey);

    return Math.max(pttl, 0);
  }

  /**
   * Records an upstream 429 and, when enabled, trips the breaker if the
   * consecutive-429 threshold is reached. The caller is responsible for logging
   * an operator-visible warning when the returned result has `tripped: true`.
   *
   * If the breaker is already open the 429 is treated as a straggler: the
   * result has `alreadyOpen: true` (and `tripped: false`), nothing is counted
   * or escalated, and the open key's TTL is left intact. The caller should
   * defer the job for the remaining cooldown without re-logging a trip.
   */
  public async recordRateLimit(): Promise<RecordRateLimitResult> {
    const client = await this.#client;

    const raw = (await client.eval(
      RECORD_RATE_LIMIT_SCRIPT,
      4,
      this.#consecutiveKey,
      this.#openKey,
      this.#levelKey,
      this.#successKey,
      String(this.#threshold),
      String(CIRCUIT_BREAKER_ESCALATED_TRIP_THRESHOLD),
      String(this.#baseCooldownMs),
      String(this.#maxCooldownMs),
      String(CIRCUIT_BREAKER_LEVEL_TTL_MS),
      String(CIRCUIT_BREAKER_COUNTER_TTL_MS),
      this.enabled ? "1" : "0",
      // Lua integer returns arrive as JS numbers over ioredis.
    )) as [number, number, number, number, number];

    const [consecutive429s, tripped, cooldownMs, level, alreadyOpen] = raw;

    return {
      consecutive429s,
      tripped: tripped === 1,
      alreadyOpen: alreadyOpen === 1,
      cooldownMs,
      level,
    };
  }

  /**
   * Records a successful upstream response: resets the consecutive-429 counter
   * and, after sustained success, de-escalates the breaker. The caller is
   * responsible for logging recovery when the result has `recovered: true`.
   */
  public async recordSuccess(): Promise<RecordSuccessResult> {
    const client = await this.#client;

    const raw = (await client.eval(
      RECORD_SUCCESS_SCRIPT,
      3,
      this.#consecutiveKey,
      this.#levelKey,
      this.#successKey,
      String(CIRCUIT_BREAKER_SUCCESS_STREAK_TO_RESET_LEVEL),
      String(CIRCUIT_BREAKER_LEVEL_TTL_MS),
    )) as [number, number];

    const [level, recovered] = raw;

    return {
      level,
      recovered: recovered === 1,
    };
  }
}
