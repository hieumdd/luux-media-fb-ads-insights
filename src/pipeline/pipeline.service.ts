import { Storage } from '@google-cloud/storage';
import { BigQuery } from '@google-cloud/bigquery';
import { NdJson } from 'json-nd';

import { getLogger } from '../logging.service';
import { createTasks } from '../cloud-tasks.service';
import { getAccounts } from '../facebook/account.service';
import { Pipeline } from './pipeline.interface';
import { CreatePipelineTasksBody, FacebookRequestOptions } from './pipeline.request.dto';
import * as pipelines from './pipeline.const';

const logger = getLogger(__filename);

export const BUSINESS_ID = '479140315800396';
export const BUCKET = new Storage().bucket('luux-media-facebook');
export const DATASET = new BigQuery().dataset('Facebook_SRC');
const QUEUE_LOCATION = 'us-central1';
const QUEUE_NAME = 'fb-ads';

export const runPipeline = async ({ name, execute }: Pipeline, options: FacebookRequestOptions) => {
    logger.info(`Running pipeline ${name}`, options);
    return await execute({ ...options, bucket: BUCKET }).then(() => options);
};

export const createPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info(`Creating mass pipelines`, { start, end });
    const accounts = await getAccounts(BUSINESS_ID);
    const create = (pipeline: Pipeline) => {
        const payloads = accounts.map(({ account_id }) => ({
            accountId: account_id,
            pipeline: pipeline.name,
            start,
            end,
        }));
        return createTasks({
            location: QUEUE_LOCATION,
            queue: QUEUE_NAME,
            payloads,
            name: (task) => [task.pipeline, task.accountId].join('-'),
        });
    };
    return await Promise.all([
        BUCKET.file('Accounts.json').save(NdJson.stringify(accounts), { resumable: false }),
        create(pipelines.AdsPublisherPlatformInsights),
        create(pipelines.CampaignsCountryInsights),
        create(pipelines.CampaignsDevicePlatformPositionInsights),
    ]);
};

export const createCustomPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info(`Creating custom pipelines`, { start, end });
    return await createTasks({
        location: QUEUE_LOCATION,
        queue: QUEUE_NAME,
        payloads: [
            [pipelines.CampaignsAgeGenderInsights.name, '285219587325995'],
            [pipelines.CampaignsRegionInsights.name, '1064565224448567'],
            [pipelines.CampaignsRegionInsights.name, '224717170151419'],
            [pipelines.CampaignsRegionInsights.name, '220506957265195'],
            [pipelines.CampaignsRegionInsights.name, '1220093882213517'],
            [pipelines.CampaignsRegionInsights.name, '193539783445588'],
            [pipelines.CampaignsAgeGenderInsights.name, '1064565224448567'],
            [pipelines.CampaignsAgeGenderInsights.name, '224717170151419'],
            [pipelines.CampaignsAgeGenderInsights.name, '220506957265195'],
            [pipelines.CampaignsAgeGenderInsights.name, '1220093882213517'],
            [pipelines.CampaignsAgeGenderInsights.name, '193539783445588'],
            [pipelines.CampaignsAgeGenderInsights.name, '887134739259540'],
            [pipelines.CampaignsAgeGenderInsights.name, '285219587325995'],
        ].map(([pipeline, accountId]) => ({ accountId, pipeline, start, end })),
        name: (task) => [task.pipeline, task.accountId].join('-'),
    });
};
