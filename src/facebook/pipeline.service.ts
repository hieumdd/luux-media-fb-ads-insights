import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import Joi from 'joi';
import ndjson from 'ndjson';

import { createLoadStream } from '../bigquery/bigquery.service';
import { createTasks } from '../task/cloud-tasks.service';
import { getAccounts } from './account.service';
import { ReportOptions, get } from './insights.service';
import * as pipelines from './pipeline.const';

dayjs.extend(utc);

export const runPipeline = async (reportOptions: ReportOptions, pipeline_: pipelines.Pipeline) => {
    const stream = await get(reportOptions, pipeline_.insightsConfig);

    await pipeline(
        stream,
        new Transform({
            objectMode: true,
            transform: (row, _, callback) => {
                callback(null, {
                    ...Joi.attempt(row, pipeline_.validationSchema),
                    _batched_at: dayjs().toISOString(),
                });
            },
        }),
        ndjson.stringify(),
        createLoadStream({
            table: `p_${pipeline_.name}__${reportOptions.accountId}`,
            schema: [...pipeline_.schema, { name: '_batched_at', type: 'TIMESTAMP' }],
        }),
    );

    return true;
};

export type CreatePipelineTasksOptions = {
    start?: string;
    end?: string;
};

export const createPipelineTasks = async ({ start, end }: CreatePipelineTasksOptions) => {
    return getAccounts()
        .then((accounts) => {
            return Object.keys(pipelines).flatMap((pipeline) => {
                return accounts.map((accountId) => ({ accountId, start, end, pipeline }));
            });
        })
        .then((data) => createTasks(data, (task) => [task.pipeline, task.accountId].join('-')));
};
