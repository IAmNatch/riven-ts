import { UUID } from "@repo/util-plugin-sdk/schemas/utilities/uuid.schema";

import z from "zod";

import { ParseScrapeResultsSandboxedJob } from "../../../../sandboxed-jobs/jobs/parse-scrape-results/parse-scrape-results.schema.ts";
import { createFlowJobBuilder } from "../../../../utilities/create-flow-job-builder.ts";
import { createFlowSchema } from "../../../../utilities/create-flow-schema.ts";

export const ScrapeItemFlow = createFlowSchema("scrape-item", {
  children: ParseScrapeResultsSandboxedJob.shape.output,
  input: z.object({
    id: UUID,
    /**
     * The attempt (`job.attemptsMade`) that the current set of plugin scrape
     * children was created for. When BullMQ retries the job, `attemptsMade` moves
     * past this and the processor enqueues fresh children before reading results.
     * Absent on the first attempt (and on jobs enqueued before this field existed).
     */
    scrapeAttempt: z.int().nonnegative().optional(),
  }),
});

export type ScrapeItemFlow = z.infer<typeof ScrapeItemFlow>;

export const scrapeItemProcessorSchema = ScrapeItemFlow.shape.processor;

export const createScrapeItemJob = createFlowJobBuilder(ScrapeItemFlow);
