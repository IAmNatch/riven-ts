import { Movie } from "@repo/util-plugin-sdk/dto/entities";
import { parse } from "@repo/util-rank-torrent-name/parser";

import { faker } from "@faker-js/faker";
import { UnrecoverableError, WaitingChildrenError } from "bullmq";
import { randomUUID } from "node:crypto";
import { expect, vi } from "vitest";

import { it } from "../../../../../__tests__/test-context.ts";
import * as settingsModule from "../../../../../utilities/settings.ts";
import { settings } from "../../../../../utilities/settings.ts";
import { scrapeItemProcessor } from "./scrape-item.processor.ts";

import type { MainRunnerMachineIntake } from "../../../../../state-machines/main-runner/index.ts";

it("throws an UnrecoverableError if the item cannot be found", async ({
  createMockJob,
  mockSentryScope,
  services,
}) => {
  const job = await createMockJob({ id: randomUUID() });

  vi.spyOn(job, "getChildrenValues").mockResolvedValue({});

  await expect(async () =>
    scrapeItemProcessor(
      {
        job,
        scope: mockSentryScope,
      },
      {
        sendEvent: vi.fn<MainRunnerMachineIntake>(),
        services,
        plugins: new Map(),
      },
    ),
  ).rejects.toThrow(
    new UnrecoverableError(`MediaItem with id ${job.data.id} not found`),
  );
});

it("throws a MediaItemScrapeErrorNoStreamsFound error if no streams are found", async ({
  createMockJob,
  mockSentryScope,
  seeders: { seedIndexedMovie },
  services,
}) => {
  const indexedMovie = await seedIndexedMovie();

  const job = await createMockJob({ id: indexedMovie.movie.id });

  vi.spyOn(job, "getChildrenValues").mockResolvedValue({});

  await expect(async () =>
    scrapeItemProcessor(
      {
        job,
        scope: mockSentryScope,
      },
      {
        sendEvent: vi.fn<MainRunnerMachineIntake>(),
        services,
        plugins: new Map(),
      },
    ),
  ).rejects.toThrow(/no streams returned from scrapers/iu);
});

it("sends a 'riven.media-item.scrape.error.no-streams-found' event if no streams are found", async ({
  createMockJob,
  mockSentryScope,
  seeders: { seedIndexedMovie },
  services,
}) => {
  const indexedMovie = await seedIndexedMovie();

  const job = await createMockJob({ id: indexedMovie.movie.id });

  vi.spyOn(job, "getChildrenValues").mockResolvedValue({});

  const sendEvent = vi.fn<MainRunnerMachineIntake>();

  await scrapeItemProcessor(
    {
      job,
      scope: mockSentryScope,
    },
    {
      sendEvent,
      services,
      plugins: new Map(),
    },
  ).catch(() => {
    /* empty */
  });

  expect(sendEvent).toHaveBeenCalledWith({
    type: "riven.media-item.scrape.error.no-streams-found",
    error: expect.any(Error),
    item: expect.any(Movie),
  });
});

it("throws an UnrecoverableError if the item exceeds the maximum failed attempts", async ({
  createMockJob,
  mockSentryScope,
  seeders: { seedIndexedMovie },
  services,
}) => {
  vi.spyOn(settingsModule, "settings", "get").mockReturnValue({
    ...settings,
    maximumFailedAttempts: 1,
  });

  const indexedMovie = await seedIndexedMovie();

  const job = await createMockJob({ id: indexedMovie.movie.id });

  vi.spyOn(job, "getChildrenValues").mockResolvedValue({});

  await expect(async () =>
    scrapeItemProcessor(
      {
        job,
        scope: mockSentryScope,
      },
      {
        sendEvent: vi.fn<MainRunnerMachineIntake>(),
        services,
        plugins: new Map(),
      },
    ),
  ).rejects.toThrow(
    new UnrecoverableError(
      `Scraping failed for ${indexedMovie.movie.fullTitle} after 1/1 attempts`,
    ),
  );
});

it('sends a "riven.media-item.scrape.success" event with the updated item if the scrape is successful', async ({
  seeders: { seedIndexedMovie },
  createMockJob,
  mockSentryScope,
  services,
  createMockJobChildKey,
}) => {
  const indexedMovie = await seedIndexedMovie();

  const job = await createMockJob({ id: indexedMovie.movie.id });

  const streamInfoHash = faker.git.commitSha();

  vi.spyOn(job, "getChildrenValues").mockResolvedValue({
    [createMockJobChildKey("scrape-item.parse-scrape-results")]: {
      id: indexedMovie.movie.id,
      results: {
        [streamInfoHash]: parse("Test Movie 2024 1080p WEB-DL"),
      },
    },
  });

  const sendEvent = vi.fn<MainRunnerMachineIntake>();

  await scrapeItemProcessor(
    {
      job,
      scope: mockSentryScope,
    },
    {
      sendEvent,
      services,
      plugins: new Map(),
    },
  );

  expect(sendEvent).toHaveBeenCalledWith({
    type: "riven.media-item.scrape.success",
    item: expect.any(Movie),
  });
});

it("enqueues fresh plugin scrapes instead of re-reading the previous attempt's results when the job is retried", async ({
  createMockJob,
  mockSentryScope,
  mockFlowProcessorContext,
  seeders: { seedIndexedMovie },
}) => {
  const indexedMovie = await seedIndexedMovie();

  const job = await createMockJob({ id: indexedMovie.movie.id });

  job.attemptsMade = 1;

  const getChildrenValues = vi.spyOn(job, "getChildrenValues");
  const moveToWaitingChildren = vi
    .spyOn(job, "moveToWaitingChildren")
    .mockResolvedValue(true);
  const scrapeItem = vi.spyOn(
    mockFlowProcessorContext.services.scraperService,
    "scrapeItem",
  );

  await expect(
    scrapeItemProcessor(
      { job, token: "token", scope: mockSentryScope },
      mockFlowProcessorContext,
    ),
  ).rejects.toThrow(WaitingChildrenError);

  expect(moveToWaitingChildren).toHaveBeenCalledWith("token");
  expect(getChildrenValues).not.toHaveBeenCalled();
  expect(scrapeItem).not.toHaveBeenCalled();
  expect(job.data).toStrictEqual({
    id: indexedMovie.movie.id,
    scrapeAttempt: 1,
  });

  const { unprocessed = [] } = await job.getDependencies();

  expect(unprocessed).toStrictEqual([
    expect.stringContaining(":scrape-item.parse-scrape-results:"),
  ]);
});

it("processes the results of the current attempt once its plugin scrapes have run", async ({
  createMockJob,
  createMockJobChildKey,
  mockSentryScope,
  mockFlowProcessorContext,
  seeders: { seedIndexedMovie },
}) => {
  const indexedMovie = await seedIndexedMovie();

  const job = await createMockJob({
    id: indexedMovie.movie.id,
    scrapeAttempt: 1,
  });

  job.attemptsMade = 1;

  const moveToWaitingChildren = vi.spyOn(job, "moveToWaitingChildren");

  vi.spyOn(job, "getChildrenValues").mockResolvedValue({
    [createMockJobChildKey("scrape-item.parse-scrape-results")]: {
      id: indexedMovie.movie.id,
      results: {
        [faker.git.commitSha()]: parse("Test Movie 2024 1080p WEB-DL"),
      },
    },
  });

  await scrapeItemProcessor(
    { job, token: "token", scope: mockSentryScope },
    mockFlowProcessorContext,
  );

  expect(moveToWaitingChildren).not.toHaveBeenCalled();
  expect(mockFlowProcessorContext.sendEvent).toHaveBeenCalledWith({
    type: "riven.media-item.scrape.success",
    item: expect.any(Movie),
  });
});

it("throws an UnrecoverableError if the item cannot be found when the job is retried", async ({
  createMockJob,
  mockSentryScope,
  mockFlowProcessorContext,
}) => {
  const job = await createMockJob({ id: randomUUID() });

  job.attemptsMade = 1;

  await expect(
    scrapeItemProcessor(
      { job, token: "token", scope: mockSentryScope },
      mockFlowProcessorContext,
    ),
  ).rejects.toThrow(
    new UnrecoverableError(`MediaItem with id ${job.data.id} not found`),
  );
});
