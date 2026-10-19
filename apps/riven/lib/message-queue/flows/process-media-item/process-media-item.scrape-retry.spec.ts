import { MediaItem, Season } from "@repo/util-plugin-sdk/dto/entities";
import { parse } from "@repo/util-rank-torrent-name/parser";

import { faker } from "@faker-js/faker";
import { Worker } from "bullmq";
import { DateTime } from "luxon";
import { expect, onTestFinished, vi } from "vitest";

import { it } from "../../../__tests__/test-context.ts";
import * as settingsModule from "../../../utilities/settings.ts";
import { settings } from "../../../utilities/settings.ts";
import { createFlowWorker } from "../../utilities/create-flow-worker.ts";
import { createQueue } from "../../utilities/create-queue.ts";
import { runSingleJob } from "../../utilities/run-single-job.ts";
import { enqueueProcessMediaItem } from "./enqueue-process-media-item.ts";
import { processMediaItemProcessor } from "./process-media-item.processor.ts";
import { ProcessMediaItemFlow } from "./process-media-item.schema.ts";
import { scrapeItemProcessor } from "./steps/scrape/scrape-item.processor.ts";
import { ScrapeItemFlow } from "./steps/scrape/scrape-item.schema.ts";

import type { MainRunnerMachineIntake } from "../../../state-machines/main-runner/index.ts";
import type { ValidPluginMap } from "../../../types/plugins.ts";
import type { MediaItemScrapeRequestedResponse } from "@repo/util-plugin-sdk/schemas/events/media-item.scrape-requested.event";
import type { ParsedData } from "@repo/util-rank-torrent-name";
import type { Job, Processor } from "bullmq";
import type { UUID } from "node:crypto";

/**
 * Runs the real `process-media-item` and `scrape-item` flows against stand-ins
 * for everything below them: the scraper plugin (`scrape`), the sandboxed
 * results parser. Nothing consumes `download-item`, so a download job that
 * gets enqueued is left there for the test to find.
 */
function startScrapeRetryHarness(
  plugins: ValidPluginMap,
  createPluginWorker: (
    eventType: "riven.media-item.scrape.requested",
    pluginName: string,
    processor: Processor,
  ) => unknown,
  scrape: () => MediaItemScrapeRequestedResponse["results"],
) {
  const workers: Worker[] = [];
  const connection = { url: settings.redisUrl };
  const sendEvent = vi.fn<MainRunnerMachineIntake>();

  onTestFinished(async () => {
    for (const worker of workers) {
      await worker.close(true);
    }
  });

  workers.push(
    createFlowWorker(
      ProcessMediaItemFlow,
      processMediaItemProcessor,
      sendEvent,
      plugins,
    ).worker,
    createFlowWorker(
      ScrapeItemFlow,
      scrapeItemProcessor,
      sendEvent,
      plugins,
      {},
      {
        // Retry immediately instead of waiting out the 30m+ cooldown.
        settings: { backoffStrategy: () => 0 },
      },
    ).worker,
  );

  const pluginScrape = vi.fn<
    (
      job: Job<{ item: { id: UUID } }>,
    ) => Promise<MediaItemScrapeRequestedResponse>
  >(async (job) =>
    Promise.resolve({
      id: job.data.item.id,
      results: scrape(),
    }),
  );

  createPluginWorker(
    "riven.media-item.scrape.requested",
    "@repo/plugin-test",
    pluginScrape as Processor,
  );

  workers.push(
    new Worker(
      "scrape-item.parse-scrape-results",
      async (job: Job<{ id: UUID }>) => {
        const results: Record<string, ParsedData> = {};
        const children =
          await job.getChildrenValues<MediaItemScrapeRequestedResponse>();

        for (const child of Object.values(children)) {
          for (const [hash, title] of Object.entries(child.results)) {
            results[hash] = parse(title);
          }
        }

        return { id: job.data.id, results };
      },
      { connection },
    ),
  );

  return { pluginScrape };
}

it("re-runs the scrapers on each retry and proceeds to download once a later scrape finds streams", async ({
  em,
  indexedMovieContext: { indexedMovie },
  mockFlowProcessorContext: { plugins },
  createPluginWorker,
}) => {
  const infoHash = faker.git.commitSha();
  let scrapes = 0;

  const { pluginScrape } = startScrapeRetryHarness(
    plugins,
    createPluginWorker,
    () => {
      scrapes += 1;

      // Nothing has been uploaded yet on the first two attempts.
      return scrapes < 3
        ? {}
        : { [infoHash]: `${indexedMovie.title} 2024 1080p WEB-DL` };
    },
  );

  await enqueueProcessMediaItem({ id: indexedMovie.id, fanOut: false });

  await vi.waitFor(
    async () => {
      const downloadJobs = await createQueue("download-item").getJobs();

      expect(downloadJobs.map(({ data }) => data)).toStrictEqual([
        { id: indexedMovie.id },
      ]);
    },
    { timeout: 10_000, interval: 100 },
  );

  expect(pluginScrape).toHaveBeenCalledTimes(3);

  const item = await em
    .fork()
    .findOneOrFail(MediaItem, indexedMovie.id, { populate: ["streams"] });

  expect(item.state).toBe("scraped");
  expect(item.failedScrapeAttempts).toBe(0);
  expect(
    item.streams.getItems().map(({ infoHash: hash }) => hash),
  ).toStrictEqual([infoHash]);
});

it("fails the item after maximumFailedAttempts scrapes that all find nothing", async ({
  em,
  indexedMovieContext: { indexedMovie },
  mockFlowProcessorContext: { plugins },
  createPluginWorker,
}) => {
  vi.spyOn(settingsModule, "settings", "get").mockReturnValue({
    ...settings,
    maximumFailedAttempts: 3,
  });

  const { pluginScrape } = startScrapeRetryHarness(
    plugins,
    createPluginWorker,
    () => ({}),
  );

  const [processJob] =
    (await enqueueProcessMediaItem({ id: indexedMovie.id, fanOut: false })) ??
    [];

  expect.assert(processJob);

  await expect(runSingleJob(processJob.job)).rejects.toThrow(
    /failed to scrape after all attempts/u,
  );

  expect(pluginScrape).toHaveBeenCalledTimes(3);

  const item = await em.fork().findOneOrFail(MediaItem, indexedMovie.id);

  expect(item.state).toBe("failed");
  expect(item.failedScrapeAttempts).toBe(3);
});

it("scrapes an exhausted season again once a new episode airs, and proceeds to download", async ({
  em,
  factories: { showFactory, seasonFactory, episodeFactory, mediaEntryFactory },
  mockFlowProcessorContext: { plugins },
  createPluginWorker,
}) => {
  const now = DateTime.utc().startOf("minute");
  const airDate = now.plus({ days: 1 });
  const indexedAt = now.toJSDate();

  const show = await showFactory.createOne({
    status: "continuing",
    releaseDate: now.minus({ weeks: 2 }).toJSDate(),
    indexedAt,
  });

  const season = await seasonFactory.createOne({
    show,
    itemRequest: show.itemRequest,
    number: 1,
    releaseDate: now.minus({ weeks: 2 }).toJSDate(),
    indexedAt,
  });

  const completedEpisode = await episodeFactory.createOne({
    season,
    show,
    itemRequest: show.itemRequest,
    number: 1,
    releaseDate: now.minus({ weeks: 1 }).toJSDate(),
    indexedAt,
  });

  completedEpisode.filesystemEntries.add(
    mediaEntryFactory.makeOne({ mediaItem: completedEpisode }),
  );

  const newEpisode = await episodeFactory.createOne({
    season,
    show,
    itemRequest: show.itemRequest,
    number: 2,
    releaseDate: airDate.toJSDate(),
    indexedAt,
  });

  // Every scrape before the new episode was uploaded counted as a failure.
  season.failedScrapeAttempts = settings.maximumFailedAttempts;

  await em.flush();

  expect(season.state).toBe("partially_completed");
  expect(newEpisode.state).toBe("unreleased");

  const infoHash = faker.git.commitSha();

  const { pluginScrape } = startScrapeRetryHarness(
    plugins,
    createPluginWorker,
    () => ({ [infoHash]: `${show.title} S01E02 1080p WEB-DL` }),
  );

  const [exhaustedJob] =
    (await enqueueProcessMediaItem({ id: season.id, fanOut: false })) ?? [];

  expect.assert(exhaustedJob);

  await expect(runSingleJob(exhaustedJob.job)).rejects.toThrow(
    /has exhausted all scrape attempts/u,
  );

  expect(pluginScrape).not.toHaveBeenCalled();

  // The new episode airs. Only `Date` is faked so BullMQ keeps real timers.
  vi.useFakeTimers({
    toFake: ["Date"],
    now: airDate.plus({ hours: 1 }).toJSDate(),
  });

  newEpisode.year = null; // Trigger a change on the entity to update the state

  await em.flush();

  vi.useRealTimers();

  expect(newEpisode.state).toBe("indexed");

  await expect(
    em.fork().findOneOrFail(Season, season.id),
  ).resolves.toMatchObject({ failedScrapeAttempts: 0 });

  await enqueueProcessMediaItem({ id: season.id, fanOut: false });

  await vi.waitFor(
    async () => {
      const downloadJobs = await createQueue("download-item").getJobs();

      expect(downloadJobs.map(({ data }) => data)).toStrictEqual([
        { id: season.id },
      ]);
    },
    { timeout: 10_000, interval: 100 },
  );

  expect(pluginScrape).toHaveBeenCalledOnce();

  const scrapedSeason = await em
    .fork()
    .findOneOrFail(Season, season.id, { populate: ["streams"] });

  expect(scrapedSeason.failedScrapeAttempts).toBe(0);
  expect(
    scrapedSeason.streams.getItems().map(({ infoHash: hash }) => hash),
  ).toStrictEqual([infoHash]);
});
