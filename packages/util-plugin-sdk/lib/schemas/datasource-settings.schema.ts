import { z } from "zod";

import type { RateLimiterOptions } from "bullmq";

/**
 * Shared, env-configurable settings for every plugin datasource.
 *
 * These keys are merged into every plugin's `settingsSchema` when its settings
 * are parsed (see {@link PluginSettings.set}), so any plugin can override its
 * datasource behaviour via the standard
 * `RIVEN_PLUGIN_SETTING__<PREFIX>__<key>` environment variable mechanism
 * without having to re-declare the keys.
 *
 * Keys are intentionally left `.optional()` (no `.default()`): a value is only
 * present here when the operator explicitly sets the environment variable, so
 * the precedence chain `base default < per-datasource code override < env
 * setting` is preserved by the `BaseDataSource` constructor.
 *
 * Note that a single plugin can expose multiple datasources; a per-plugin
 * environment value therefore applies to **all** of that plugin's datasources.
 */
export const datasourceSettingsShape = {
  datasourceConcurrency: z.coerce
    .number()
    .int()
    .min(1)
    .optional()
    .describe(
      "How many requests this plugin's datasource worker(s) process simultaneously. Applies to every datasource exposed by the plugin. Defaults to 200 (or the datasource's own code override).",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceRateLimitMax: z.coerce
    .number()
    .int()
    .min(1)
    .optional()
    .describe(
      "The maximum number of requests this plugin's datasource may make per `datasourceRateLimitDuration` window. Requires `datasourceRateLimitDuration` to also be set to take effect. Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceRateLimitDuration: z.coerce
    .number()
    .int()
    .min(1)
    .optional()
    .describe(
      "The rate limiter window, in milliseconds, over which `datasourceRateLimitMax` requests are allowed. Requires `datasourceRateLimitMax` to also be set to take effect. Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceMaxRateLimitRetries: z.coerce
    .number()
    .int()
    .min(0)
    .optional()
    .describe(
      "How many times a single request is re-queued after receiving an HTTP 429 (Too Many Requests) response before it is abandoned. Prevents a persistently rate-limited endpoint from being retried forever. Defaults to 5. Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceBreakerThreshold: z.coerce
    .number()
    .int()
    .optional()
    .describe(
      "How many *consecutive* HTTP 429 responses across all of this plugin's datasource requests trip the circuit breaker, which then pauses ALL upstream requests for a cooldown. This bounds the aggregate request rate during a backfill so a persistently rate-limited upstream is left quiet long enough to lift a per-IP ban. Set to 0 (or a negative value) to disable the breaker entirely. Defaults to 5. Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceBreakerCooldownSeconds: z.coerce
    .number()
    .int()
    .min(1)
    .optional()
    .describe(
      "The base circuit-breaker cooldown, in seconds, applied the first time the breaker trips. Each subsequent trip that happens before the datasource recovers doubles the cooldown (escalating backoff), capped at `datasourceBreakerMaxCooldownSeconds`. Defaults to 300 (5 minutes). Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceBreakerMaxCooldownSeconds: z.coerce
    .number()
    .int()
    .min(1)
    .optional()
    .describe(
      "The ceiling, in seconds, for the escalating circuit-breaker cooldown. Defaults to 7200 (2 hours). Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
  datasourceMaxRateLimitBackoffSeconds: z.coerce
    .number()
    .int()
    .min(1)
    .optional()
    .describe(
      "The ceiling, in seconds, for the escalating backoff applied to a single request after an HTTP 429 response that carries no (or an invalid) `Retry-After` header. The wait starts at 10s and doubles per consecutive 429, capped at this value. A valid `Retry-After` header is always honoured as-is instead. Defaults to 300 (5 minutes). Applies to every datasource exposed by the plugin.",
    )
    .meta({ "wiki.section": "datasource" }),
} as const;

export const DatasourceSettings = z.object(datasourceSettingsShape);

export type DatasourceSettings = z.infer<typeof DatasourceSettings>;

/**
 * Resolves the effective rate limiter options from an (optional) code-level
 * override and (optional) environment-provided max/duration values.
 *
 * A limiter is only produced when both `max` and `duration` are known, since
 * BullMQ requires both. Environment values take precedence over the code
 * override on a per-field basis.
 */
export function resolveRateLimiterOptions(
  codeOverride: RateLimiterOptions | undefined,
  envMax: number | undefined,
  envDuration: number | undefined,
): RateLimiterOptions | undefined {
  const max = envMax ?? codeOverride?.max;
  const duration = envDuration ?? codeOverride?.duration;

  if (max === undefined || duration === undefined) {
    return undefined;
  }

  return { max, duration };
}
