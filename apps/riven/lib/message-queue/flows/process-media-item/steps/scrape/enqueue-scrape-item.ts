import { MediaItemScrapeRequestedEvent } from "@repo/util-plugin-sdk/schemas/events/media-item.scrape-requested.event";

import { settings } from "../../../../../utilities/settings.ts";
import { createParseScrapeResultsJob } from "../../../../sandboxed-jobs/jobs/parse-scrape-results/parse-scrape-results.schema.ts";
import { createPluginFlowJob } from "../../../../utilities/create-flow-plugin-job.ts";
import { flow } from "../../../producer.ts";
import { createScrapeItemJob } from "./scrape-item.schema.ts";

import type { RivenPlugin } from "@repo/util-plugin-sdk";
import type { FlowJob, ParentOptions } from "bullmq";

export interface EnqueueScrapeItemInput {
  item: MediaItemScrapeRequestedEvent["item"];
  subscribers: RivenPlugin[];
  parent: ParentOptions;
  isRootItem: boolean;
}

/**
 * Builds the `parse-scrape-results` node and its per-plugin scrape jobs.
 *
 * These are the jobs that actually call the scrapers, so a scrape attempt only
 * queries them again if it gets a fresh copy of this node.
 */
function createParseScrapeResultsNode(
  item: MediaItemScrapeRequestedEvent["item"],
  subscribers: RivenPlugin[],
  opts: FlowJob["opts"] = {},
) {
  const childNodes = subscribers.map((plugin) =>
    createPluginFlowJob(
      MediaItemScrapeRequestedEvent,
      `Scrape ${item.fullTitle}`,
      plugin.name.description ?? "unknown",
      { item },
      { ignoreDependencyOnFailure: true },
    ),
  );

  return createParseScrapeResultsJob(
    "Parse scrape results",
    {
      id: item.id,
      title: item.fullTitle,
    },
    {
      children: childNodes,
      opts: {
        ...opts,
        ignoreDependencyOnFailure: true,
      },
    },
  );
}

export async function enqueueScrapeItem({
  item,
  subscribers,
  parent,
  isRootItem,
}: EnqueueScrapeItemInput) {
  const attempts = isRootItem
    ? settings.maximumFailedAttempts - item.failedScrapeAttempts
    : 1;

  if (attempts <= 0) {
    return null;
  }

  const node = createScrapeItemJob(
    `Scraping ${item.fullTitle}`,
    { id: item.id },
    {
      children: [createParseScrapeResultsNode(item, subscribers)],
      opts: {
        parent,
        continueParentOnFailure: true,
        attempts,
        backoff: {
          type: "custom",
        },
      },
    },
  );

  return flow.add(node);
}

export interface EnqueueScrapeItemRetryInput {
  item: MediaItemScrapeRequestedEvent["item"];
  subscribers: RivenPlugin[];
  /** The `scrape-item` job that is being retried. */
  parent: ParentOptions;
}

/**
 * Adds a fresh set of plugin scrape jobs to an existing `scrape-item` job.
 *
 * BullMQ retries a parent job without re-running its completed children, so
 * without this every retry of `scrape-item` would re-read the results of the
 * first attempt instead of querying the scrapers again.
 */
export async function enqueueScrapeItemRetry({
  item,
  subscribers,
  parent,
}: EnqueueScrapeItemRetryInput) {
  return flow.add(createParseScrapeResultsNode(item, subscribers, { parent }));
}
