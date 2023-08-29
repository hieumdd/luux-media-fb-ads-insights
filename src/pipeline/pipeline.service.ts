import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import dayjs from '../dayjs';
import { logger } from '../logging.service';
import { createLoadStream } from '../bigquery.service';
import { createTasks } from '../cloud-tasks.service';
import { getAccounts } from '../facebook/account.service';
import { CreatePipelineTasksBody, PipelineOptions } from './pipeline.request.dto';
import * as pipelines from './pipeline.const';

export const runInsightsPipeline = async (
    pipeline_: pipelines.Pipeline,
    options: PipelineOptions,
) => {
    logger.info({ fn: 'runInsightsPipeline', pipeline: pipeline_.name, options });

    const stream = await pipeline_.getExtractStream(options);

    return pipeline(
        stream,
        new Transform({
            objectMode: true,
            transform: (row, _, callback) => {
                const { value, error } = pipeline_.validationSchema.validate(row);

                if (error) {
                    callback(error);
                    return;
                }

                callback(null, { ...value, _batched_at: dayjs().utc().toISOString() });
            },
        }),
        ndjson.stringify(),
        createLoadStream(
            {
                schema: [
                    ...pipeline_.loadConfig.schema,
                    { name: '_batched_at', type: 'TIMESTAMP' },
                ],
                writeDisposition: pipeline_.loadConfig.writeDisposition,
            },
            `p_${pipeline_.name}__${options.accountId}`,
        ),
    ).then(() => ({ pipeline: pipeline_.name, ...options }));
};

export const runAdsPipeline = async (options: PipelineOptions) => {
    logger.info({ fn: 'runAdsPipeline' });

    const stream = await pipelines.ADS.getExtractStream(options);

    return pipeline(
        stream,
        new Transform({
            objectMode: true,
            transform: (row, _, callback) => {
                const { value, error } = pipelines.ADS.validationSchema.validate(row);

                if (error) {
                    callback(error);
                    return;
                }

                callback(null, { ...value, _batched_at: dayjs().utc().toISOString() });
            },
        }),
        ndjson.stringify(),
        createLoadStream(
            {
                schema: [...pipelines.ADS.schema, { name: '_batched_at', type: 'TIMESTAMP' }],
                writeDisposition: 'WRITE_APPEND',
            },
            `p_${pipelines.ADS.name}__${options.accountId}`,
        ),
    ).then(() => ({ pipeline: pipelines.ADS.name, ...options }));
};

export const createInsightsPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info({ fn: 'createInsightsPipelineTasks', options: { start, end } });

    const accounts = await getAccounts();

    return Promise.all([
        [
            pipelines.ADS_PUBLISHER_PLATFORM_INSIGHTS,
            pipelines.CAMPAIGNS_COUNTRY_INSIGHTS,
            pipelines.CAMPAIGNS_DEVICE_PLATFORM_POSITION_INSIGHTS,
        ]
            .map((pipeline) => {
                return accounts.map(({ account_id }) => ({
                    accountId: account_id,
                    start,
                    end,
                    pipeline,
                }));
            })
            .map((data) => createTasks(data, (task) => [task.pipeline, task.accountId].join('-'))),
        pipeline(
            Readable.from(accounts),
            ndjson.stringify(),
            createLoadStream(
                {
                    schema: [
                        { name: 'account_name', type: 'STRING' },
                        { name: 'account_id', type: 'INT64' },
                    ],
                    writeDisposition: 'WRITE_TRUNCATE',
                },
                'Accounts',
            ),
        ),
    ]);
};

export const createAdsPipelineTasks = async (_: CreatePipelineTasksBody) => {
    logger.info({ fn: 'createAdsPipelineTasks' });

    const accountIds = [
        '1064565224448567',
        '224717170151419',
        '220506957265195',
        '1220093882213517',
        '193539783445588',
    ];

    return createTasks(
        accountIds.map((accountId) => ({ accountId, pipeline: 'ADS' })),
        (task) => [task.pipeline, task.accountId].join('-'),
    );
};
