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
