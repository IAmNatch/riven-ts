import { RESTDataSource } from "@apollo/datasource-rest";
import {
  Queue,
  QueueEvents,
  RateLimitError,
  UnrecoverableError,
  Worker,
} from "bullmq";
import { DateTime, Duration } from "luxon";
import { URL } from "node:url";
import z from "zod";

import { benchmark } from "../helpers/benchmark.ts";
import {
  DatasourceSettings,
  resolveRateLimiterOptions,
} from "../schemas/datasource-settings.schema.ts";
import { json } from "../validation/json.ts";
import { urlSearchParamsCodec } from "../validation/url-search-params-parser.ts";
import { dataSourceContext } from "./context.ts";

import type {
  AugmentedRequest,
  DataSourceConfig,
  DataSourceFetchResult,
  DataSourceRequest,
  RequestOptions,
} from "@apollo/datasource-rest";
import type { KeyvAdapter } from "@apollo/utils.keyvadapter";
import type {
  ConnectionOptions,
  ParentOptions,
  RateLimiterOptions,
  Telemetry,
  Job,
} from "bullmq";
import type EventEmitter from "node:events";
import type { Promisable } from "type-fest";
import type { Logger } from "winston";

interface FetchJobInput {
  path: string;
  incomingRequest: DataSourceRequest | undefined;
  /**
   * Used to determine how to decode the request body.
   */
  bodyType: "json" | "url-search-params" | undefined;
  params: string;
  /**
   * The number of times this job has been re-queued after receiving an HTTP 429
   * (Too Many Requests) response. Used to bound rate-limit retries so a
   * persistently rate-limited endpoint is not retried forever.
   *
   * Persisted on the job (via `job.updateData`) so the count survives each
   * re-queue. Absent/`undefined` is treated as `0`.
   */
  rateLimitRetries?: number;
}

type FetchResponse<T = unknown> = Pick<
  DataSourceFetchResult<T>,
  "parsedBody"
> & {
  response: {
    ok: boolean;
    status: number;
    statusText: string;
    headers: Record<string, string>;
  };
} & (
    | {
        success: true;
        responseTime: number;
        responseFromCache: boolean | undefined;
      }
    | { success: false }
  );

export class DataSourceHTTPError extends Error {
  public override name = "DataSourceHTTPError";

  public response!: DataSourceFetchResult<never>["response"];

  public constructor(response: DataSourceFetchResult<never>["response"]) {
    super(
      `${response.status.toString()} ${response.statusText} for ${response.url}`,
    );

    this.response = response;
  }
}

export interface BaseDataSourceConfig<
  T extends Record<string, unknown>,
> extends Omit<DataSourceConfig, "logger"> {
  settings: T;
  pluginSymbol: symbol;
  requestAttempts?: number;
  requestBackoffDelay?: number;
  logger: Logger;
  connection: ConnectionOptions;
  telemetry: Telemetry;
  userAgent: string;
  /**
   * Per-datasource code-level override for the worker concurrency.
   *
   * Because subclass field initializers only run *after* `super()` returns —
   * by which point the worker has already been constructed — datasource
   * subclasses must pass their concurrency override here rather than declaring
   * it as a class field.
   *
   * Precedence (increasing): base default (200) < this override < env setting
   * (`datasourceConcurrency`).
   */
  concurrency?: number;
  /**
   * Per-datasource code-level override for the queue rate limiter.
   *
   * Must be passed through the constructor (not a subclass field) for the same
   * ordering reason as {@link concurrency}.
   *
   * Precedence (increasing): base default (none) < this override < env setting
   * (`datasourceRateLimitMax` / `datasourceRateLimitDuration`).
   */
  rateLimiterOptions?: RateLimiterOptions;
  /**
   * Per-datasource code-level override for the maximum number of rate-limit
   * (HTTP 429) retries before a request is abandoned.
   *
   * Precedence (increasing): base default (5) < this override < env setting
   * (`datasourceMaxRateLimitRetries`).
   */
  maxRateLimitRetries?: number;
}

export abstract class BaseDataSource<
  DataSourceSettings extends Record<string, unknown>,
> extends RESTDataSource {
  public abstract override readonly baseURL: string;

  public readonly serviceName: string;
  public readonly settings: DataSourceSettings;

  public override readonly logger: Logger;

  /**
   * The effective queue rate limiter options for this datasource.
   *
   * Resolved in the constructor (before the worker is built) from the base
   * default (none/unthrottled), the per-datasource code override
   * ({@link BaseDataSourceConfig.rateLimiterOptions}), and the per-plugin env
   * settings (`datasourceRateLimitMax` / `datasourceRateLimitDuration`), in
   * increasing order of precedence.
   *
   * NOTE: this MUST be resolved via the constructor config — a subclass field
   * initializer would run only after `super()` (and the worker) has already
   * been built, so field-level overrides never reach the worker.
   */
  protected readonly rateLimiterOptions?: RateLimiterOptions | undefined;

  /**
   * Controls the concurrency (i.e. how many jobs it works on simultaneously) for the datasource worker.
   *
   * The default is `200`, as the worker only handles I/O operations, so a high concurrency can be used.
   *
   * This works in combination with the queue rate limiter to control the overall request rate from the datasource.
   *
   * If your API throws a lot of timeout errors, try reducing this value.
   *
   * Resolved in the constructor from the base default, the per-datasource code
   * override ({@link BaseDataSourceConfig.concurrency}) and the per-plugin env
   * setting (`datasourceConcurrency`), in increasing order of precedence.
   *
   * @see https://docs.bullmq.io/guide/parallelism-and-concurrency#how-to-best-use-bullmqs-concurrency-then
   *
   * @default 200
   */
  protected readonly concurrency: number;

  /**
   * The maximum number of times a single request is re-queued after receiving
   * an HTTP 429 (Too Many Requests) response before it is abandoned.
   *
   * This bounds rate-limit retries so that a persistently rate-limited endpoint
   * cannot be retried forever (which would also keep the upstream IP throttled).
   * Once exceeded, the request fails terminally and the caller's error handling
   * proceeds.
   *
   * Resolved in the constructor from the base default, the per-datasource code
   * override ({@link BaseDataSourceConfig.maxRateLimitRetries}) and the
   * per-plugin env setting (`datasourceMaxRateLimitRetries`), in increasing
   * order of precedence.
   *
   * @default 5
   */
  protected readonly maxRateLimitRetries: number;

  readonly #requestAttempts: number;
  readonly #requestBackoffDelay: number;

  readonly #queueId: string;
  readonly #queueEvents: QueueEvents;
  public queue: Queue<FetchJobInput, FetchResponse>;
  public worker: Worker<FetchJobInput, FetchResponse>;

  readonly #keyv: KeyvAdapter;
  readonly #keyvPrefix = "httpcache:";

  /**
   * A set of HTTP status codes that should not be treated as fatal errors.
   *
   * When a response with one of these status codes is received, the datasource
   * will attempt to re-request the data after a delay, rather than immediately throwing an error.
   *
   * For all other 4xx and 5xx status codes, no retries will be made.
   */
  readonly #nonFatalStatusCodes = new Set([408, 425, 429, 500, 502, 503, 504]);

  public constructor({
    pluginSymbol,
    settings,
    requestAttempts = 3,
    requestBackoffDelay = 10_000,
    concurrency,
    rateLimiterOptions,
    maxRateLimitRetries,
    connection,
    telemetry,
    userAgent,
    ...apolloDataSourceOptions
  }: BaseDataSourceConfig<DataSourceSettings>) {
    super(apolloDataSourceOptions);

    this.#keyv = apolloDataSourceOptions.cache as KeyvAdapter;

    this.serviceName = this.constructor.name;
    this.#requestAttempts = requestAttempts;
    this.#requestBackoffDelay = requestBackoffDelay;

    // Resolve datasource knobs BEFORE the worker is built.
    //
    // Precedence (increasing): base default < per-datasource code override
    // (constructor config) < per-plugin env setting (parsed out of `settings`).
    //
    // These MUST flow through the constructor: a subclass field initializer
    // runs only after `super()` returns, by which point the worker below has
    // already read `this.concurrency` / `this.rateLimiterOptions`, so field
    // overrides would silently never take effect.
    const datasourceSettings =
      DatasourceSettings.safeParse(settings).data ?? {};

    this.concurrency =
      datasourceSettings.datasourceConcurrency ?? concurrency ?? 200;
    this.maxRateLimitRetries =
      datasourceSettings.datasourceMaxRateLimitRetries ??
      maxRateLimitRetries ??
      5;
    this.rateLimiterOptions = resolveRateLimiterOptions(
      rateLimiterOptions,
      datasourceSettings.datasourceRateLimitMax,
      datasourceSettings.datasourceRateLimitDuration,
    );

    this.#queueId = `${pluginSymbol.description ?? "unknown"}-${this.serviceName}-fetch-queue`;
    this.queue = new Queue(this.#queueId, {
      connection,
      telemetry,
    });

    this.#queueEvents = new QueueEvents(this.#queueId, { connection });

    this.worker = new Worker(
      this.#queueId,
      async (job, _token, signal) => {
        await job.log(`Processing request for ${job.data.path}`);

        try {
          const {
            timeTaken,
            result: { parsedBody, response, responseFromCache },
          } = await benchmark(async () => {
            this.logger.silly(
              [
                `[${this.serviceName}] Initiating request to ${new URL(job.data.path, this.baseURL).toString()}`,
                ...(job.data.params ? [`?${job.data.params}`] : []),
              ].join(""),
            );

            this.#decodeRequestBody(job);

            job.data.incomingRequest ??= {};
            job.data.incomingRequest.signal = signal;
            job.data.incomingRequest.params = urlSearchParamsCodec.decode(
              job.data.params,
            );
            job.data.incomingRequest.headers ??= {};
            job.data.incomingRequest.headers["user-agent"] = userAgent;

            return super.fetch(job.data.path, job.data.incomingRequest);
          });

          await job.log(
            `Request completed in ${(timeTaken / 1000).toFixed(2)} seconds`,
          );

          return {
            success: true,
            parsedBody,
            response: {
              ok: response.ok,
              status: response.status,
              statusText: response.statusText,
              headers: Object.fromEntries(response.headers),
            },
            responseTime: timeTaken,
            responseFromCache,
          };
        } catch (error) {
          // A RateLimitError is thrown from `didEncounterRateLimit` on HTTP 429.
          // BullMQ re-queues these WITHOUT consuming an attempt (via
          // `moveLimitedBackToWait`), so without an explicit ceiling a
          // persistently rate-limited endpoint would be retried forever — and
          // the retries themselves keep the upstream IP throttled, so it never
          // self-clears. Bound the number of re-queues with a dedicated,
          // persisted counter.
          if (error instanceof RateLimitError) {
            const rateLimitRetries = job.data.rateLimitRetries ?? 0;

            if (rateLimitRetries >= this.maxRateLimitRetries) {
              this.logger.warn(
                `[${this.serviceName}] giving up after ${rateLimitRetries.toString()} rate-limit retries for ${job.data.path}`,
              );

              // UnrecoverableError fails the job terminally regardless of
              // remaining attempts, so `fetch`'s `waitUntilFinished` rejects and
              // the caller's error handling proceeds gracefully.
              throw new UnrecoverableError(
                `[${this.serviceName}] Exceeded maximum rate-limit retries (${this.maxRateLimitRetries.toString()}) for ${job.data.path}`,
              );
            }

            // Persist the incremented counter BEFORE rethrowing so it survives
            // the re-queue, then rethrow the RateLimitError so BullMQ routes it
            // through `moveLimitedBackToWait` (no attempt consumed).
            await job.updateData({
              ...job.data,
              rateLimitRetries: rateLimitRetries + 1,
            });

            throw error;
          }

          const hasRemainingAttempts =
            job.attemptsStarted !== this.#requestAttempts;

          const isFatalHttpStatusCode =
            error instanceof DataSourceHTTPError &&
            !this.#nonFatalStatusCodes.has(error.response.status);

          const shouldRetry = hasRemainingAttempts && !isFatalHttpStatusCode;

          if (shouldRetry) {
            throw error;
          }

          if (error instanceof DataSourceHTTPError) {
            return {
              success: false,
              parsedBody: null,
              response: {
                headers: Object.fromEntries(error.response.headers),
                ok: error.response.ok,
                status: error.response.status,
                statusText: error.response.statusText,
              },
            };
          }

          throw error;
        }
      },
      {
        connection,
        ...(this.rateLimiterOptions && { limiter: this.rateLimiterOptions }),
        telemetry,
        concurrency: Math.max(1, Math.floor(this.concurrency)),
        removeOnComplete: {
          age: 60,
          count: 5000,
        },
        removeOnFail: {
          age: 60 * 60 * 24,
          count: 5000,
        },
      },
    );

    this.logger = apolloDataSourceOptions.logger;

    for (const resource of [this.queue, this.#queueEvents, this.worker]) {
      (resource as EventEmitter).on("error", (error: unknown) => {
        this.logger.error(
          `${this.#queueId} ${resource.constructor.name} error`,
          { err: error },
        );
      });
    }

    this.settings = settings;
  }

  #decodeRequestBody(job: Job<FetchJobInput, FetchResponse>) {
    const { bodyType } = job.data;

    if (!bodyType || !job.data.incomingRequest?.body) {
      return;
    }

    if (typeof job.data.incomingRequest.body !== "string") {
      throw new UnrecoverableError("Unable to decode non-string request body.");
    }

    if (bodyType === "url-search-params") {
      job.data.incomingRequest.body = urlSearchParamsCodec.decode(
        job.data.incomingRequest.body,
      );

      return;
    }

    job.data.incomingRequest.body = json(
      z.record(z.string(), z.unknown()),
    ).decode(job.data.incomingRequest.body);
  }

  #parseHTTPDate(dateString: string): number | null {
    try {
      return DateTime.fromHTTP(dateString).diffNow().toMillis();
    } catch {
      return null;
    }
  }

  #parseRetryAfterHeader(retryAfterHeader: string | number): number | null {
    if (typeof retryAfterHeader === "number") {
      return retryAfterHeader;
    }

    const httpDate = this.#parseHTTPDate(retryAfterHeader);

    if (httpDate !== null) {
      return httpDate;
    }

    const retryAfterSeconds = Math.trunc(Number(retryAfterHeader));

    if (Number.isNaN(retryAfterSeconds)) {
      return null;
    }

    // If the Retry-After header is a string, it's the number of **seconds** to wait
    return retryAfterSeconds * 1000;
  }

  #urlSearchParamsFromRecord(
    params: Record<string, string | undefined> | undefined,
  ): URLSearchParams {
    const usp = new URLSearchParams();

    if (params) {
      for (const [name, value] of Object.entries(params)) {
        if (value !== undefined) {
          usp.set(name, value);
        }
      }
    }

    return usp;
  }

  // Generate an outgoing request, after applying any request modifications.
  // This is mostly copied from RESTDataSource, as there was no native way to determine
  // whether a request is cached without actually performing subsequent fetch.
  async #createAugmentedRequest(
    path: string,
    incomingRequest?: DataSourceRequest,
  ): Promise<{
    augmentedRequest: AugmentedRequest;
    url: URL;
  }> {
    const augmentedRequest: AugmentedRequest = {
      ...incomingRequest,
      params:
        incomingRequest?.params instanceof URLSearchParams
          ? incomingRequest.params
          : this.#urlSearchParamsFromRecord(incomingRequest?.params),
      headers: incomingRequest?.headers ?? {},
    };

    augmentedRequest.method ??= "GET";

    await this.willSendRequest?.(path, augmentedRequest);

    const downcasedHeaders: Record<string, string> = {};

    // Map incoming headers to lower-case headers
    for (const [key, value] of Object.entries(augmentedRequest.headers)) {
      downcasedHeaders[key.toLowerCase()] = value;
    }

    augmentedRequest.headers = downcasedHeaders;

    const url = await this.resolveURL(path, augmentedRequest);

    // Append params to existing params in the path
    for (const [name, value] of augmentedRequest.params) {
      url.searchParams.append(name, value);
    }

    if (this.shouldJSONSerializeBody(augmentedRequest.body)) {
      augmentedRequest.body = JSON.stringify(augmentedRequest.body);

      // If Content-Type header has not been previously set, set to application/json
      augmentedRequest.headers["content-type"] ??= "application/json";
    }

    return {
      augmentedRequest,
      url,
    };
  }

  #determineRequestBodyType(body: unknown) {
    if (!body) {
      return;
    }

    if (body instanceof URLSearchParams) {
      return "url-search-params";
    }

    if (typeof body === "object") {
      return "json";
    }

    if (typeof body === "string") {
      try {
        JSON.parse(body);
      } catch {
        throw new UnrecoverableError(
          "Unable to determine the request body type: invalid JSON string.",
        );
      }

      return "json";
    }

    throw new UnrecoverableError("Unable to determine the request body type.");
  }

  async #createRequestJob(
    path: string,
    request: AugmentedRequest,
    cacheKey: string,
    parentOptions?: ParentOptions,
  ) {
    const bodyType = this.#determineRequestBodyType(request.body);

    if (bodyType === "url-search-params") {
      request.body = urlSearchParamsCodec.encode(
        request.body as URLSearchParams,
      );
    }

    return this.queue.add(
      cacheKey,
      {
        path,
        incomingRequest: request as DataSourceRequest,
        bodyType,
        params: urlSearchParamsCodec.encode(request.params),
      },
      {
        ...(parentOptions && { parent: parentOptions }),
        attempts: this.#requestAttempts,
        backoff: {
          type: "exponential",
          delay: this.#requestBackoffDelay,
          jitter: 0.5,
        },
        removeDependencyOnFailure: true,
      },
    );
  }

  public override async fetch<T>(
    path: string,
    incomingRequest?: DataSourceRequest,
  ): Promise<DataSourceFetchResult<T>> {
    const { augmentedRequest, url } = await this.#createAugmentedRequest(
      path,
      incomingRequest,
    );

    const cacheKey = this.cacheKeyFor(url, augmentedRequest as never);

    const isCached = Boolean(
      await this.#keyv.get(`${this.#keyvPrefix}${cacheKey}`),
    );

    if (isCached) {
      // If we have a cached response, bypass the message queue and fetch directly
      return super.fetch(path, augmentedRequest as DataSourceRequest);
    }

    const context = dataSourceContext.getStore();

    const jobParentOptions = context?.job.id
      ? ({
          id: context.job.id,
          queue: context.job.queueQualifiedName,
        } satisfies ParentOptions)
      : undefined;

    const job = await this.#createRequestJob(
      path,
      augmentedRequest,
      cacheKey,
      jobParentOptions,
    );

    const result = await job.waitUntilFinished(this.#queueEvents);

    const clonedResponse = new Response(null, result.response);

    if (!result.success) {
      throw new DataSourceHTTPError(clonedResponse);
    }

    const commonResponseFields = {
      response: clonedResponse,
      // The following fields aren't used by our application,
      // but must be included to satisfy the return type.
      responseFromCache: result.responseFromCache ?? false,
      requestDeduplication: undefined as never,
      httpCache: {
        cacheWritePromise: Promise.resolve(),
      },
    } as const satisfies Pick<
      DataSourceFetchResult<T>,
      "responseFromCache" | "requestDeduplication" | "httpCache" | "response"
    >;

    const logMessage = result.responseFromCache
      ? `[${this.serviceName}] Returned cached response for ${augmentedRequest.method ?? "GET"} ${url.toString()}`
      : `[${this.serviceName}] HTTP ${result.response.status.toString()} response for ${augmentedRequest.method ?? "GET"} ${url.toString()} in ${(result.responseTime / 1000).toFixed(2)} seconds`;

    if (!result.response.ok) {
      throw new DataSourceHTTPError(clonedResponse);
    }

    this.logger.http(logMessage);

    return {
      parsedBody: result.parsedBody as T,
      ...commonResponseFields,
    };
  }

  public override async throwIfResponseIsError({
    request,
    response,
  }: {
    url: URL;
    request: RequestOptions;
    response: DataSourceFetchResult<unknown>["response"];
    parsedBody: unknown;
  }) {
    if (response.ok) {
      return;
    }

    if (response.status === 429) {
      const defaultWaitMs = 10_000;
      const waitMs = this.#parseRetryAfterHeader(
        response.headers.get("Retry-After") ?? "",
      );

      await this.didEncounterRateLimit(
        request,
        response,
        waitMs === null || waitMs <= 0 ? defaultWaitMs : waitMs,
      );
    }

    throw new DataSourceHTTPError(response);
  }

  protected override didEncounterError(
    error: Error,
    _request: RequestOptions,
    url: URL,
  ): void {
    if (error instanceof RateLimitError) {
      return;
    }

    if (error instanceof UnrecoverableError) {
      return;
    }

    if (error.name === "AbortError") {
      return;
    }

    if ("code" in error && error.code === "ETIMEDOUT") {
      this.logger.warn(
        `[${this.serviceName}] Request to ${url.toString()} timed out.`,
      );

      return;
    }

    this.logger.error(`[${this.serviceName}] API Error for ${url.toString()}`, {
      err: error,
    });
  }

  protected didEncounterRateLimit(
    _request: RequestOptions,
    response: DataSourceFetchResult<unknown>["response"],
    waitMs: number,
  ): Promisable<void>;

  protected async didEncounterRateLimit(
    _request: RequestOptions,
    response: DataSourceFetchResult<unknown>["response"],
    waitMs: number,
  ): Promise<void> {
    await this.queue.rateLimit(waitMs);

    const formattedWaitTime = Duration.fromMillis(waitMs).rescale().toHuman();

    this.logger.warn(
      `[${this.serviceName}] Received 429 Too Many Requests response for ${response.url}; retrying after ${formattedWaitTime}`,
    );

    throw Worker.RateLimitError();
  }

  public abstract validate(): Promisable<boolean>;
}

export type { RateLimiterOptions } from "bullmq";
