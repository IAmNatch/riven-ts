import { KeyvAdapter } from "@apollo/utils.keyvadapter";
import { HttpResponse, http } from "msw";
import { randomUUID } from "node:crypto";
import { describe, expect, vi } from "vitest";
import { createLogger } from "winston";
import { z } from "zod";

import { it as baseIt } from "../__tests__/test-context.ts";
import {
  DatasourceSettings,
  resolveRateLimiterOptions,
} from "../schemas/datasource-settings.schema.ts";
import { PluginSettings } from "../utilities/plugin-settings.ts";
import { BaseDataSource } from "./index.ts";

import type { BaseDataSourceConfig } from "./index.ts";
import type { RateLimiterOptions } from "bullmq";
import type { Promisable } from "type-fest";

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * Polls `predicate` until it resolves truthy or the timeout elapses. Used by
 * the circuit-breaker tests, which observe asynchronous Redis state changes
 * produced by the background worker.
 */
async function waitFor(
  predicate: () => Promisable<boolean>,
  {
    timeout = 15_000,
    interval = 25,
  }: { timeout?: number; interval?: number } = {},
): Promise<void> {
  const deadline = Date.now() + timeout;

  for (;;) {
    if (await predicate()) {
      return;
    }

    if (Date.now() > deadline) {
      throw new Error(`waitFor timed out after ${timeout.toString()}ms`);
    }

    await sleep(interval);
  }
}

class TestDataSource extends BaseDataSource<Record<string, unknown>> {
  public override baseURL = "https://example.com/api";

  public override validate(): Promisable<boolean> {
    return true;
  }
}

/**
 * Exposes the (protected) resolved datasource knobs for precedence testing.
 */
class ExposedDataSource extends BaseDataSource<Record<string, unknown>> {
  public override baseURL = "https://example.com/api";

  public get resolvedConcurrency(): number {
    return this.concurrency;
  }

  public get resolvedMaxRateLimitRetries(): number {
    return this.maxRateLimitRetries;
  }

  public get resolvedRateLimiterOptions(): RateLimiterOptions | undefined {
    return this.rateLimiterOptions;
  }

  public exposedEscalatingBackoffMs(consecutive429s: number): number {
    return this.escalatingBackoffMs(consecutive429s);
  }

  public override validate(): Promisable<boolean> {
    return true;
  }
}

const it = baseIt
  .extend("keyvCache", async ({ redisClient: { url } }, { onCleanup }) => {
    const { default: KeyvRedis, Keyv } = await import("@keyv/redis");

    const keyv = new Keyv<string>(new KeyvRedis(url.toString()));

    onCleanup(async () => {
      await keyv.disconnect();
    });

    return new KeyvAdapter(keyv as never);
  })
  .extend("dataSourceConfig", ({ redisClient, keyvCache }, { onCleanup }) => {
    const logger = createLogger({ silent: true });

    const config = {
      settings: {},
      connection: redisClient.client,
      cache: keyvCache,
      logger,
      pluginSymbol: Symbol.for(`@repo/plugin-test-${randomUUID()}`),
      telemetry: undefined as never, // Telemetry isn't needed here; force disable
      userAgent: "mock-user-agent",
    } satisfies BaseDataSourceConfig<Record<string, unknown>>;

    onCleanup(async () => {
      await redisClient.client.flushall();
    });

    return config;
  });

it("enqueues subsequent jobs to the same URL separately", async ({
  server,
  dataSourceConfig,
}) => {
  server.use(
    http.get("**/endpoint", () => HttpResponse.json({ success: false }), {
      once: true,
    }),
    http.get("**/endpoint", () => HttpResponse.json({ success: true }), {
      once: true,
    }),
  );

  const dataSource = new TestDataSource(dataSourceConfig);

  const firstRequest = await dataSource.fetch("endpoint");

  expect(firstRequest.parsedBody).toStrictEqual({ success: false });

  const secondRequest = await dataSource.fetch("endpoint");

  expect(secondRequest.parsedBody).toStrictEqual({ success: true });
});

it("bypasses the queue if a valid response is available in the cache", async ({
  server,
  dataSourceConfig,
}) => {
  server.use(
    http.get(
      "**/endpoint",
      () =>
        HttpResponse.json(
          { value: "cached-value" },
          { headers: { "Cache-Control": "max-age=3600" } },
        ),
      { once: true },
    ),
  );

  const dataSource = new TestDataSource(dataSourceConfig);
  const queueAddSpy = vi.spyOn(dataSource.queue, "add");

  await dataSource.fetch("endpoint");
  await dataSource.fetch("endpoint");

  expect(queueAddSpy).toHaveBeenCalledOnce();
});

it("does not bypass the queue if no valid response is available in the cache", async ({
  server,
  dataSourceConfig,
}) => {
  server.use(
    http.get("**/endpoint", () => HttpResponse.json({ value: "value-1" }), {
      once: true,
    }),
    http.get("**/endpoint", () => HttpResponse.json({ value: "value-2" }), {
      once: true,
    }),
  );

  const dataSource = new TestDataSource(dataSourceConfig);
  const queueAddSpy = vi.spyOn(dataSource.queue, "add");

  await dataSource.fetch("endpoint");
  await dataSource.fetch("endpoint");

  expect(queueAddSpy).toHaveBeenCalledTimes(2);
});

it("returns a cached response if available in the cache", async ({
  server,
  dataSourceConfig,
}) => {
  server.use(
    http.get(
      "**/endpoint",
      () =>
        HttpResponse.json(
          { value: "cached-value" },
          { headers: { "Cache-Control": "max-age=3600" } },
        ),
      { once: true },
    ),
  );

  const dataSource = new TestDataSource(dataSourceConfig);

  await dataSource.fetch("endpoint");

  const secondRequest = await dataSource.fetch("endpoint");

  expect(secondRequest.parsedBody).toStrictEqual({ value: "cached-value" });
});

it(
  "stops re-queueing a persistently rate-limited (429) request after maxRateLimitRetries and fails, persisting the counter across re-queues",
  { timeout: 30_000 },
  async ({ server, dataSourceConfig }) => {
    let hits = 0;

    server.use(
      http.get("**/endpoint", () => {
        hits += 1;

        return HttpResponse.json(
          { error: "rate limited" },
          { status: 429, headers: { "Retry-After": "1" } },
        );
      }),
    );

    const dataSource = new TestDataSource({
      ...dataSourceConfig,
      maxRateLimitRetries: 2,
      // Disable the circuit breaker so this exercises the per-item retry ceiling
      // in isolation (the breaker is covered by its own tests below).
      breakerThreshold: 0,
    });

    // The request should ultimately fail (reject) rather than loop forever.
    await expect(dataSource.fetch("endpoint")).rejects.toThrow(
      "Exceeded maximum rate-limit retries",
    );

    // Initial attempt + exactly maxRateLimitRetries (2) re-queues == 3 hits.
    // Proves the counter persisted across re-queues and bounded the retries.
    expect(hits).toBe(3);
  },
);

it(
  "recovers when a 429 is followed by a successful response (no attempt consumed)",
  { timeout: 30_000 },
  async ({ server, dataSourceConfig }) => {
    let hits = 0;

    server.use(
      http.get(
        "**/endpoint",
        () => {
          hits += 1;

          return HttpResponse.json(
            { error: "rate limited" },
            { status: 429, headers: { "Retry-After": "1" } },
          );
        },
        { once: true },
      ),
      http.get("**/endpoint", () => {
        hits += 1;

        return HttpResponse.json({ success: true });
      }),
    );

    const dataSource = new TestDataSource({
      ...dataSourceConfig,
      // Disable the circuit breaker so a single 429 followed by success is
      // exercised in isolation (the breaker is covered by its own tests below).
      breakerThreshold: 0,
    });

    const response = await dataSource.fetch("endpoint");

    expect(response.parsedBody).toStrictEqual({ success: true });
    expect(hits).toBe(2);
  },
);

it(
  "retries a 503 via the attempts+backoff path and then fails",
  { timeout: 30_000 },
  async ({ server, dataSourceConfig }) => {
    let hits = 0;

    server.use(
      http.get("**/endpoint", () => {
        hits += 1;

        return HttpResponse.json({ error: "unavailable" }, { status: 503 });
      }),
    );

    const dataSource = new TestDataSource({
      ...dataSourceConfig,
      requestAttempts: 2,
      requestBackoffDelay: 100,
    });

    await expect(dataSource.fetch("endpoint")).rejects.toThrow("503");

    // 503 is non-fatal: it consumes attempts (unlike 429). With 2 attempts we
    // expect exactly 2 hits, then a terminal failure — unchanged behaviour.
    expect(hits).toBe(2);
  },
);

describe("circuit breaker", () => {
  it(
    "bounds total upstream hits during a backfill storm and stays flat while open (regression)",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig, redisClient }) => {
      let hits = 0;

      server.use(
        http.get("**/endpoint", () => {
          hits += 1;

          return HttpResponse.json(
            { error: "rate limited" },
            { status: 429, headers: { "Retry-After": "1" } },
          );
        }),
      );

      const threshold = 3;

      const dataSource = new TestDataSource({
        ...dataSourceConfig,
        // Serialise the worker so the pre-trip hit count is deterministic.
        concurrency: 1,
        breakerThreshold: threshold,
        // Long enough to observe a flat plateau within the test.
        breakerCooldownSeconds: 5,
        // Do not let the per-item ceiling end jobs before the breaker trips.
        maxRateLimitRetries: 50,
      });

      const openKey = dataSource.queue.toKey("circuit-breaker:open");

      // Enqueue a large backlog; do NOT await — these hang during the cooldown,
      // exactly as parent scrape jobs would in production.
      const backlog = 20;

      for (let index = 0; index < backlog; index += 1) {
        void dataSource.fetch(`endpoint?item=${index.toString()}`).catch(() => {
          // Swallowed: these jobs never resolve within the test window.
        });
      }

      // Wait until the breaker trips.
      await waitFor(async () => (await redisClient.client.pttl(openKey)) > 0);

      // At most `threshold` upstream requests should have escaped before the
      // breaker opened (exactly `threshold` at concurrency 1).
      expect(hits).toBeGreaterThan(0);
      expect(hits).toBeLessThanOrEqual(threshold);

      const hitsAtTrip = hits;

      // The backfill-storm guarantee: while the breaker is open, ZERO further
      // upstream requests happen even though ~17 jobs remain queued.
      await sleep(1500);

      await expect(redisClient.client.pttl(openKey)).resolves.toBeGreaterThan(
        0,
      );
      expect(hits).toBe(hitsAtTrip);

      await dataSource.worker.close();
    },
  );

  it(
    "drains the backlog and resets the 429 counter once the cooldown expires",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig, redisClient }) => {
      let rateLimitedResponses = 0;
      const failFirst = 2;

      server.use(
        http.get("**/endpoint", () => {
          if (rateLimitedResponses < failFirst) {
            rateLimitedResponses += 1;

            return HttpResponse.json(
              { error: "rate limited" },
              { status: 429, headers: { "Retry-After": "1" } },
            );
          }

          return HttpResponse.json({ success: true });
        }),
      );

      const dataSource = new TestDataSource({
        ...dataSourceConfig,
        concurrency: 1,
        breakerThreshold: 2,
        breakerCooldownSeconds: 1,
      });

      const consecutiveKey = dataSource.queue.toKey(
        "circuit-breaker:consecutive-429",
      );

      const backlog = 5;

      const results = await Promise.all(
        Array.from({ length: backlog }, async (_, index) =>
          dataSource.fetch(`endpoint?item=${index.toString()}`),
        ),
      );

      // Every job in the backlog ultimately succeeds once the breaker closes.
      for (const result of results) {
        expect(result.parsedBody).toStrictEqual({ success: true });
      }

      // A successful response resets the consecutive-429 counter.
      const counter = await redisClient.client.get(consecutiveKey);

      expect(counter === null || counter === "0").toBe(true);
    },
  );

  it(
    "re-trips at a longer cooldown when a 429 arrives immediately after reopening",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig, redisClient }) => {
      server.use(
        http.get("**/endpoint", () =>
          HttpResponse.json(
            { error: "rate limited" },
            { status: 429, headers: { "Retry-After": "1" } },
          ),
        ),
      );

      const dataSource = new TestDataSource({
        ...dataSourceConfig,
        concurrency: 1,
        breakerThreshold: 2,
        breakerCooldownSeconds: 2,
        breakerMaxCooldownSeconds: 3600,
        maxRateLimitRetries: 50,
      });

      const openKey = dataSource.queue.toKey("circuit-breaker:open");
      const levelKey = dataSource.queue.toKey("circuit-breaker:level");

      const backlog = 10;

      for (let index = 0; index < backlog; index += 1) {
        void dataSource.fetch(`endpoint?item=${index.toString()}`).catch(() => {
          // Swallowed: these never resolve within the test window.
        });
      }

      // First trip => escalation level 1.
      await waitFor(
        async () => (await redisClient.client.get(levelKey)) === "1",
      );

      const firstCooldown = await redisClient.client.pttl(openKey);

      expect(firstCooldown).toBeGreaterThan(0);

      // Once the cooldown expires, the half-open probe 429s and re-trips at the
      // NEXT (escalated) level with a longer cooldown.
      await waitFor(
        async () => (await redisClient.client.get(levelKey)) === "2",
      );

      const secondCooldown = await redisClient.client.pttl(openKey);

      expect(secondCooldown).toBeGreaterThan(firstCooldown);

      await dataSource.worker.close();
    },
  );

  it(
    "does not consume a job's rate-limit retry budget while it waits for the breaker",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig, redisClient }) => {
      let rateLimitedResponses = 0;

      server.use(
        http.get("**/endpoint", () => {
          // Only the very first request 429s (tripping the breaker); everything
          // afterwards succeeds. The "victim" job therefore never receives a
          // real 429 — it is only ever deferred by the open breaker.
          if (rateLimitedResponses === 0) {
            rateLimitedResponses += 1;

            return HttpResponse.json(
              { error: "rate limited" },
              { status: 429 },
            );
          }

          return HttpResponse.json({ success: true });
        }),
      );

      const dataSource = new TestDataSource({
        ...dataSourceConfig,
        concurrency: 1,
        breakerThreshold: 1,
        breakerCooldownSeconds: 1,
        // 0 => a single REAL 429 fails a job immediately. A breaker-wait must
        // NOT count against this, or the victim below could never succeed.
        maxRateLimitRetries: 0,
      });

      const openKey = dataSource.queue.toKey("circuit-breaker:open");

      // Trigger: 429s, trips the breaker, and (maxRateLimitRetries=0) fails.
      await expect(dataSource.fetch("endpoint?trigger")).rejects.toThrow(
        "Exceeded maximum rate-limit retries",
      );

      await expect(redisClient.client.pttl(openKey)).resolves.toBeGreaterThan(
        0,
      );

      // Victim: enqueued while the breaker is open. It is deferred by the
      // breaker (breaker-wait re-queues that do NOT touch rateLimitRetries), not
      // failed, and succeeds once the breaker closes.
      const victim = await dataSource.fetch("endpoint?victim");

      expect(victim.parsedBody).toStrictEqual({ success: true });
    },
  );

  it(
    "keeps the breaker open for a freshly-constructed datasource on the same Redis (restart persistence)",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig, redisClient }) => {
      let hits = 0;

      server.use(
        http.get("**/endpoint", () => {
          hits += 1;

          return HttpResponse.json({ error: "rate limited" }, { status: 429 });
        }),
      );

      const config = {
        ...dataSourceConfig,
        concurrency: 1,
        breakerThreshold: 1,
        breakerCooldownSeconds: 3,
        maxRateLimitRetries: 50,
      } satisfies BaseDataSourceConfig<Record<string, unknown>>;

      const instanceA = new TestDataSource(config);
      const openKey = instanceA.queue.toKey("circuit-breaker:open");

      // Trip the breaker on instance A.
      void instanceA.fetch("endpoint?trigger").catch(() => {
        // Swallowed.
      });

      await waitFor(async () => (await redisClient.client.pttl(openKey)) > 0);

      expect(hits).toBe(1);

      // Simulate a process restart: stop A's worker, then build a brand-new
      // datasource against the SAME Redis / queue id. It has no in-memory
      // knowledge of the trip — only the persisted Redis state.
      await instanceA.worker.close();

      const instanceB = new TestDataSource(config);

      // A job enqueued on the fresh instance must not reach the upstream while
      // the (persisted) breaker is still open.
      void instanceB.fetch("endpoint?after-restart").catch(() => {
        // Swallowed.
      });

      await sleep(1500);

      expect(hits).toBe(1);
      await expect(redisClient.client.pttl(openKey)).resolves.toBeGreaterThan(
        0,
      );

      await instanceB.worker.close();
    },
  );
});

describe("escalating 429 backoff", () => {
  it("scales the no-Retry-After wait as 10s * 2^(n-1) capped at the configured max", ({
    dataSourceConfig,
  }) => {
    const dataSource = new ExposedDataSource({
      ...dataSourceConfig,
      maxRateLimitBackoffSeconds: 300,
    });

    expect(dataSource.exposedEscalatingBackoffMs(1)).toBe(10_000);
    expect(dataSource.exposedEscalatingBackoffMs(2)).toBe(20_000);
    expect(dataSource.exposedEscalatingBackoffMs(3)).toBe(40_000);
    expect(dataSource.exposedEscalatingBackoffMs(4)).toBe(80_000);
    // 10s * 2^5 = 320s, capped to the 300s ceiling.
    expect(dataSource.exposedEscalatingBackoffMs(6)).toBe(300_000);
    expect(dataSource.exposedEscalatingBackoffMs(100)).toBe(300_000);
  });

  it("respects a custom backoff cap", ({ dataSourceConfig }) => {
    const dataSource = new ExposedDataSource({
      ...dataSourceConfig,
      maxRateLimitBackoffSeconds: 30,
    });

    expect(dataSource.exposedEscalatingBackoffMs(1)).toBe(10_000);
    expect(dataSource.exposedEscalatingBackoffMs(2)).toBe(20_000);
    // 10s * 2^2 = 40s, capped to the 30s ceiling.
    expect(dataSource.exposedEscalatingBackoffMs(3)).toBe(30_000);
  });

  it(
    "honours an explicit Retry-After header verbatim instead of the escalating backoff",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig }) => {
      server.use(
        http.get("**/endpoint", () =>
          HttpResponse.json(
            { error: "rate limited" },
            { status: 429, headers: { "Retry-After": "3" } },
          ),
        ),
      );

      const dataSource = new TestDataSource({
        ...dataSourceConfig,
        // Isolate the backoff computation from the breaker.
        breakerThreshold: 0,
        maxRateLimitRetries: 1,
      });

      const rateLimitSpy = vi.spyOn(dataSource.queue, "rateLimit");

      void dataSource.fetch("endpoint").catch(() => {
        // Swallowed.
      });

      await waitFor(() => rateLimitSpy.mock.calls.length > 0);

      // Retry-After: 3 => a 3s wait, NOT the 10s escalating base.
      expect(rateLimitSpy).toHaveBeenCalledWith(3000);

      await dataSource.worker.close();
    },
  );

  it(
    "uses the escalating backoff when no Retry-After header is present",
    { timeout: 30_000 },
    async ({ server, dataSourceConfig }) => {
      server.use(
        http.get("**/endpoint", () =>
          HttpResponse.json({ error: "rate limited" }, { status: 429 }),
        ),
      );

      const dataSource = new TestDataSource({
        ...dataSourceConfig,
        breakerThreshold: 0,
        maxRateLimitRetries: 1,
      });

      const rateLimitSpy = vi.spyOn(dataSource.queue, "rateLimit");

      void dataSource.fetch("endpoint").catch(() => {
        // Swallowed.
      });

      await waitFor(() => rateLimitSpy.mock.calls.length > 0);

      // First 429 with no Retry-After => the 10s escalating base.
      expect(rateLimitSpy).toHaveBeenCalledWith(10_000);

      await dataSource.worker.close();
    },
  );
});

describe("datasource settings precedence", () => {
  it("uses base defaults when neither code override nor env settings are provided", ({
    dataSourceConfig,
  }) => {
    const dataSource = new ExposedDataSource(dataSourceConfig);

    expect(dataSource.resolvedConcurrency).toBe(200);
    expect(dataSource.resolvedMaxRateLimitRetries).toBe(5);
    expect(dataSource.resolvedRateLimiterOptions).toBeUndefined();
  });

  it("applies per-datasource code overrides over the base defaults", ({
    dataSourceConfig,
  }) => {
    const dataSource = new ExposedDataSource({
      ...dataSourceConfig,
      concurrency: 4,
      maxRateLimitRetries: 3,
      rateLimiterOptions: { max: 2, duration: 1000 },
    });

    expect(dataSource.resolvedConcurrency).toBe(4);
    expect(dataSource.resolvedMaxRateLimitRetries).toBe(3);
    expect(dataSource.resolvedRateLimiterOptions).toStrictEqual({
      max: 2,
      duration: 1000,
    });
  });

  it("lets env-provided settings win over code overrides (and coerces strings)", ({
    dataSourceConfig,
  }) => {
    const dataSource = new ExposedDataSource({
      ...dataSourceConfig,
      // Code overrides (lower precedence than env settings below):
      concurrency: 4,
      maxRateLimitRetries: 3,
      rateLimiterOptions: { max: 2, duration: 1000 },
      // Env-provided settings (highest precedence), as raw strings:
      settings: {
        datasourceConcurrency: "10",
        datasourceMaxRateLimitRetries: "7",
        datasourceRateLimitMax: "3",
        datasourceRateLimitDuration: "2000",
      },
    });

    expect(dataSource.resolvedConcurrency).toBe(10);
    expect(dataSource.resolvedMaxRateLimitRetries).toBe(7);
    expect(dataSource.resolvedRateLimiterOptions).toStrictEqual({
      max: 3,
      duration: 2000,
    });
  });
});

describe("datasource settings schema", () => {
  it("coerces env string values into numbers and ignores unrelated keys", () => {
    const parsed = DatasourceSettings.parse({
      datasourceConcurrency: "12",
      datasourceRateLimitMax: "3",
      datasourceRateLimitDuration: "1500",
      datasourceMaxRateLimitRetries: "4",
      datasourceBreakerThreshold: "6",
      datasourceBreakerCooldownSeconds: "120",
      datasourceBreakerMaxCooldownSeconds: "3600",
      datasourceMaxRateLimitBackoffSeconds: "90",
      filter: "some-plugin-specific-value",
    });

    expect(parsed).toStrictEqual({
      datasourceConcurrency: 12,
      datasourceRateLimitMax: 3,
      datasourceRateLimitDuration: 1500,
      datasourceMaxRateLimitRetries: 4,
      datasourceBreakerThreshold: 6,
      datasourceBreakerCooldownSeconds: 120,
      datasourceBreakerMaxCooldownSeconds: 3600,
      datasourceMaxRateLimitBackoffSeconds: 90,
    });
  });

  it("only builds a limiter when both max and duration are known", () => {
    expect(
      resolveRateLimiterOptions(undefined, undefined, undefined),
    ).toBeUndefined();
    expect(resolveRateLimiterOptions(undefined, 3, undefined)).toBeUndefined();
    expect(
      resolveRateLimiterOptions({ max: 2, duration: 1000 }, 5, undefined),
    ).toStrictEqual({ max: 5, duration: 1000 });
    expect(resolveRateLimiterOptions(undefined, 3, 2000)).toStrictEqual({
      max: 3,
      duration: 2000,
    });
  });

  it("merges the shared datasource keys into an arbitrary plugin schema when parsing env settings", () => {
    const schema = z.object({ filter: z.string().default("default-filter") });
    const logger = createLogger({ silent: true });

    const pluginSettings = new PluginSettings(
      {
        RIVEN_PLUGIN_SETTING__MY_PLUGIN__datasourceConcurrency: "8",
        RIVEN_PLUGIN_SETTING__MY_PLUGIN__datasourceMaxRateLimitRetries: "9",
      },
      ["MY_PLUGIN"],
      logger,
      false,
    );

    pluginSettings.set("MY_PLUGIN", schema);

    expect(pluginSettings.get(schema)).toStrictEqual({
      filter: "default-filter",
      datasourceConcurrency: 8,
      datasourceMaxRateLimitRetries: 9,
    });
  });
});
