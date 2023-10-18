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

export const runPipeline = async (pipeline_: pipelines.Pipeline, options: PipelineOptions) => {
    logger.info({ fn: 'runPipeline', pipeline: pipeline_.name, options });

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

export const createInsightsPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info({ fn: 'createInsightsPipelineTasks', options: { start, end } });

    const accounts = await getAccounts();

    return Promise.all([
        [
            'ADS_PUBLISHER_PLATFORM_INSIGHTS',
            'CAMPAIGNS_COUNTRY_INSIGHTS',
            'CAMPAIGNS_DEVICE_PLATFORM_POSITION_INSIGHTS',
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

export const createCustomPipelineTasks = async (_: CreatePipelineTasksBody) => {
    logger.info({ fn: 'createCustomPipelineTasks' });

    const customs = [
        ['ADS', '1064565224448567'],
        ['ADS', '224717170151419'],
        ['ADS', '220506957265195'],
        ['ADS', '1220093882213517'],
        ['ADS', '193539783445588'],
        ['CampaignsAgeGenderInsights', '285219587325995'],
    ] as const;

    return createTasks(
        customs.map(([pipeline, accountId]) => ({ accountId, pipeline })),
        (task) => [task.pipeline, task.accountId].join('-'),
    );
};
