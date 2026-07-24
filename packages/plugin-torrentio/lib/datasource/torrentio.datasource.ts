import { BaseDataSource, getStremioScrapeConfig } from "@repo/util-plugin-sdk";
import { z } from "@repo/util-plugin-sdk/validation";

import type { TorrentioSettings } from "../torrentio-settings.schema.ts";
import type { BaseDataSourceConfig, ParamsFor } from "@repo/util-plugin-sdk";
import type { MediaItemScrapeRequestedEvent } from "@repo/util-plugin-sdk/schemas/events/media-item.scrape-requested.event";

const TorrentioScrapeResponse = z.object({
  streams: z.array(
    z.object({
      title: z.string(),
      infoHash: z.hash("sha1"),
    }),
  ),
});

class TorrentioAPIError extends Error {
  public override name = "TorrentioAPIError";
}

export class TorrentioAPI extends BaseDataSource<TorrentioSettings> {
  public override baseURL = "http://torrentio.strem.fun/";
  public override serviceName = "Torrent.io";

  get #filter() {
    return this.settings.filter;
  }

  public constructor(config: BaseDataSourceConfig<TorrentioSettings>) {
    // Torrentio (torrentio.strem.fun) enforces a strict per-IP rate limit.
    //
    // These overrides MUST be passed through the constructor rather than
    // declared as class fields: a subclass field initializer runs only after
    // `super()` (and the worker) has already been built, so a field-level
    // override would never reach the worker and the datasource would run
    // unthrottled at the default concurrency of 200.
    //
    // The limiter (not concurrency) is the binding constraint: concurrency is
    // kept above the per-window release rate so the limiter gates throughput.
    super({
      ...config,
      rateLimiterOptions: { max: 2, duration: 1000 },
      concurrency: 4,
    });
  }

  public override async validate() {
    try {
      // Implement your own validation logic here
      await this.get("validate");

      return true;
    } catch {
      return false;
    }
  }

  public async scrape({
    item,
  }: ParamsFor<MediaItemScrapeRequestedEvent>): Promise<
    Record<string, string>
  > {
    try {
      if (!item.imdbId) {
        throw new TorrentioAPIError(
          "IMDB ID is required for Torrentio scraping",
        );
      }

      const { identifier, imdbId, scrapeType } =
        await getStremioScrapeConfig(item);

      const response = await this.get<unknown>(
        `${this.#filter}/stream/${scrapeType}/${imdbId}${identifier ?? ""}.json`,
      );

      const parsed = TorrentioScrapeResponse.parse(response);

      if (parsed.streams.length === 0) {
        this.logger.info(
          `No streams found for item ${item.fullTitle} (IMDB: ${item.imdbId})`,
        );

        return {};
      }

      const torrents: Record<string, string> = {};

      for (const stream of parsed.streams) {
        if (!stream.infoHash) {
          continue;
        }

        const [streamTitle = ""] = stream.title.split("\n👤");
        const [rawTitle = ""] = streamTitle.split("\n");

        if (!rawTitle) {
          continue;
        }

        torrents[stream.infoHash] = rawTitle;
      }

      const torrentsCount = Object.keys(torrents).length;

      if (torrentsCount > 0) {
        this.logger.info(
          `Found ${torrentsCount.toString()} torrents from ${this.serviceName} for ${item.fullTitle} (IMDB: ${item.imdbId})`,
        );
      } else {
        this.logger.info(
          `No torrents found from ${this.serviceName} for ${item.fullTitle} (IMDB: ${item.imdbId})`,
        );
      }

      return torrents;
    } catch (error: unknown) {
      this.logger.error(
        `Failed to scrape ${item.fullTitle} (IMDB: ${item.imdbId ?? "N/A"})`,
        { err: error },
      );

      return {};
    }
  }
}
