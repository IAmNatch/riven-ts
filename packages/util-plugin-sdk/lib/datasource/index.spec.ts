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
import { BaseDataSource, DataSourceHTTPError } from "./index.ts";

import type { BaseDataSourceConfig } from "./index.ts";
import type { RateLimiterOptions } from "bullmq";
import type { Promisable } from "type-fest";

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

it("deduplicates concurrent GET requests to the same URL", async ({
  server,
  dataSourceConfig,
}) => {
  let requestCount = 0;

  server.use(
    http.get("**/endpoint", () => {
      requestCount += 1;

      return HttpResponse.json({ value: requestCount });
    }),
  );

  const dataSource = new TestDataSource(dataSourceConfig);
  const queueAddSpy = vi.spyOn(dataSource.queue, "add");

  const responses = await Promise.all([
    dataSource.fetch("endpoint"),
    dataSource.fetch("endpoint"),
    dataSource.fetch("endpoint"),
  ]);

  expect(queueAddSpy).toHaveBeenCalledOnce();

  expect(requestCount).toBe(1);

  for (const response of responses) {
    expect(response.parsedBody).toStrictEqual({ value: 1 });
  }
});

it("does not deduplicate concurrent GET requests with different params", async ({
  server,
  dataSourceConfig,
}) => {
  server.use(
    http.get("**/endpoint", ({ request }) =>
      HttpResponse.json({
        page: new URL(request.url).searchParams.get("page"),
      }),
    ),
  );

  const dataSource = new TestDataSource(dataSourceConfig);
  const queueAddSpy = vi.spyOn(dataSource.queue, "add");

  const [firstPage, secondPage] = await Promise.all([
    dataSource.fetch("endpoint", { params: { page: "1" } }),
    dataSource.fetch("endpoint", { params: { page: "2" } }),
  ]);

  expect(queueAddSpy).toHaveBeenCalledTimes(2);

  expect(firstPage.parsedBody).toStrictEqual({ page: "1" });
  expect(secondPage.parsedBody).toStrictEqual({ page: "2" });
});

it("does not deduplicate concurrent POST requests", async ({
  server,
  dataSourceConfig,
}) => {
  let requestCount = 0;

  server.use(
    http.post("**/endpoint", () => {
      requestCount += 1;

      return HttpResponse.json({ success: true });
    }),
  );

  const dataSource = new TestDataSource(dataSourceConfig);
  const queueAddSpy = vi.spyOn(dataSource.queue, "add");

  await Promise.all([
    dataSource.fetch("endpoint", { method: "POST", body: { value: 1 } }),
    dataSource.fetch("endpoint", { method: "POST", body: { value: 1 } }),
  ]);

  expect(queueAddSpy).toHaveBeenCalledTimes(2);
  expect(requestCount).toBe(2);
});

it("rejects all deduplicated requests when the shared request fails, then allows a retry", async ({
  server,
  dataSourceConfig,
}) => {
  server.use(
    http.get("**/endpoint", () => new HttpResponse(null, { status: 404 }), {
      once: true,
    }),
    http.get("**/endpoint", () => HttpResponse.json({ success: true }), {
      once: true,
    }),
  );

  const dataSource = new TestDataSource(dataSourceConfig);
  const queueAddSpy = vi.spyOn(dataSource.queue, "add");

  const results = await Promise.allSettled([
    dataSource.fetch("endpoint"),
    dataSource.fetch("endpoint"),
  ]);

  expect(queueAddSpy).toHaveBeenCalledOnce();

  for (const result of results) {
    expect.assert(result.status === "rejected");

    expect(result.reason).toBeInstanceOf(DataSourceHTTPError);
  }

  const retry = await dataSource.fetch("endpoint");

  expect(queueAddSpy).toHaveBeenCalledTimes(2);
  expect(retry.parsedBody).toStrictEqual({ success: true });
});

it("performs a single cache lookup for concurrent GET requests to a cached URL", async ({
  server,
  dataSourceConfig,
  keyvCache,
}) => {
  server.use(
    http.get(
      "**/cached-endpoint",
      () =>
        HttpResponse.json(
          { value: "cached-value" },
          { headers: { "Cache-Control": "max-age=3600" } },
        ),
      { once: true },
    ),
  );

  const dataSource = new TestDataSource(dataSourceConfig);

  // Warm the cache
  await dataSource.fetch("cached-endpoint");

  const queueAddSpy = vi.spyOn(dataSource.queue, "add");
  const cacheGetSpy = vi.spyOn(keyvCache, "get");

  const responses = await Promise.all([
    dataSource.fetch("cached-endpoint"),
    dataSource.fetch("cached-endpoint"),
    dataSource.fetch("cached-endpoint"),
  ]);

  // Served from the cache, so the queue must be bypassed
  expect(queueAddSpy).not.toHaveBeenCalled();

  // A single cache lookup reads the key twice: once for the `isCached` check in
  // BaseDataSource, and once when RESTDataSource's HTTPCache reads the response.
  expect(cacheGetSpy).toHaveBeenCalledTimes(2);

  // Check that none of the cache lookups were to keys other than "cached-endpoint"
  expect(cacheGetSpy).not.toHaveBeenCalledWith(
    expect.not.stringContaining("cached-endpoint"),
  );

  for (const response of responses) {
    expect(response.parsedBody).toStrictEqual({ value: "cached-value" });
  }
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

    const dataSource = new TestDataSource(dataSourceConfig);

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
      filter: "some-plugin-specific-value",
    });

    expect(parsed).toStrictEqual({
      datasourceConcurrency: 12,
      datasourceRateLimitMax: 3,
      datasourceRateLimitDuration: 1500,
      datasourceMaxRateLimitRetries: 4,
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
