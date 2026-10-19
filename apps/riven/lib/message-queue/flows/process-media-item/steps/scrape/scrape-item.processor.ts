import { MediaItemScrapeError } from "@repo/util-plugin-sdk/schemas/events/media-item.scrape.error.event";
import { MediaItemScrapeErrorIncorrectState } from "@repo/util-plugin-sdk/schemas/events/media-item.scrape.error.incorrect-state.event";
import { MediaItemScrapeErrorNoStreamsFound } from "@repo/util-plugin-sdk/schemas/events/media-item.scrape.error.no-streams-found.event";

import { NotFoundError, ValidationError } from "@mikro-orm/core";
import { UnrecoverableError, WaitingChildrenError } from "bullmq";
import chalk from "chalk";
import assert from "node:assert";

import { getPluginEventSubscribers } from "../../../../../state-machines/main-runner/utilities/get-plugin-event-subscribers.ts";
import { logger } from "../../../../../utilities/logger/logger.ts";
import { settings } from "../../../../../utilities/settings.ts";
import { createJobParentConfig } from "../../../../utilities/create-job-parent-config.ts";
import { filterChildrenValues } from "../../../../utilities/filter-children-values.ts";
import { enqueueScrapeItemRetry } from "./enqueue-scrape-item.ts";
import { scrapeItemProcessorSchema } from "./scrape-item.schema.ts";

import type { ParsedData } from "@repo/util-rank-torrent-name";

export const scrapeItemProcessor = scrapeItemProcessorSchema.implementAsync(
  async (
    { job, token },
    { sendEvent, services: { mediaItemService, scraperService }, plugins },
  ) => {
    // A BullMQ retry re-runs this processor but not its completed children, so
    // the plugin scrapes from the previous attempt have to be enqueued again.
    // Otherwise every retry re-reads the same (empty) results without ever
    // querying the scrapers.
    if (job.attemptsMade > (job.data.scrapeAttempt ?? 0)) {
      assert.ok(token, "Job token is required");

      try {
        const { id, type } = await mediaItemService.getMediaItemById(
          job.data.id,
        );
        const item = await scraperService.getItemToScrape(id, type);

        logger.info(
          `Re-scraping ${chalk.bold(item.fullTitle)} (attempt ${(item.failedScrapeAttempts + 1).toString()}/${settings.maximumFailedAttempts.toString()})`,
        );

        await enqueueScrapeItemRetry({
          item,
          subscribers: getPluginEventSubscribers(
            "riven.media-item.scrape.requested",
            plugins,
          ),
          parent: createJobParentConfig(job),
        });
      } catch (error) {
        if (error instanceof NotFoundError) {
          throw new UnrecoverableError(
            `MediaItem with id ${job.data.id} not found`,
          );
        }

        if (error instanceof ValidationError) {
          throw new UnrecoverableError(error.message);
        }

        throw error;
      }

      await job.updateData({
        ...job.data,
        scrapeAttempt: job.attemptsMade,
      });

      if (await job.moveToWaitingChildren(token)) {
        throw new WaitingChildrenError();
      }
    }

    const children = filterChildrenValues(
      await job.getChildrenValues(),
      "scrape-item.parse-scrape-results",
    );

    const parsedResults: Record<string, ParsedData> = {};

    for (const scrapeResult of Object.values(children)) {
      Object.assign(parsedResults, scrapeResult.results);
    }

    try {
      const { item, error } = await scraperService.scrapeItem(
        job.data.id,
        parsedResults,
      );

      if (item.state === "failed") {
        throw new UnrecoverableError(
          `Scraping failed for ${item.fullTitle} after ${item.failedScrapeAttempts.toString()}/${settings.maximumFailedAttempts.toString()} attempts`,
        );
      }

      if (error) {
        throw error;
      }

      sendEvent({
        type: "riven.media-item.scrape.success",
        item,
      });
    } catch (error) {
      if (
        error instanceof MediaItemScrapeErrorIncorrectState ||
        error instanceof MediaItemScrapeError
      ) {
        sendEvent(error.payload);

        throw new UnrecoverableError(error.message);
      }

      if (error instanceof MediaItemScrapeErrorNoStreamsFound) {
        sendEvent(error.payload);
      }

      if (error instanceof NotFoundError) {
        throw new UnrecoverableError(
          `MediaItem with id ${job.data.id} not found`,
        );
      }

      throw error;
    }
  },
);
