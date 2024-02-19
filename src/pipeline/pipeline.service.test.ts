import * as pipelines from './pipeline.const';
import {
    createCustomPipelineTasks,
    createInsightsPipelineTasks,
    runPipeline,
} from './pipeline.service';

describe('pipeline', () => {
    it.each([
        pipelines.CAMPAIGNS_AGE_GENDER_INSIGHTS,
        pipelines.CAMPAIGNS_DEVICE_PLATFORM_POSITION_INSIGHTS,
        pipelines.CAMPAIGNS_COUNTRY_INSIGHTS,
        pipelines.CAMPAIGNS_REGION_INSIGHTS,
        pipelines.ADS_PUBLISHER_PLATFORM_INSIGHTS,
    ])('$.name', async (pipeline) => {
        return runPipeline(pipeline, {
            accountId: '605738303715189',
            start: '2023-12-01',
            end: '2024-02-01',
        })
            .then((results) => {
                expect(results).toBeDefined();
            })
            .catch((error) => {
                console.error({ error });
                return Promise.reject(error);
            });
    });
});

it('pispeline', async () => {
    return runPipeline(pipelines.ADS_PUBLISHER_PLATFORM_INSIGHTS, {
        accountId: '406026759976583',
        start: '2023-06-01',
        end: '2023-08-01',
    })
        .then((results) => {
            expect(results).toBeDefined();
        })
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});

it('create-tasks-insights', async () => {
    return createInsightsPipelineTasks({
        start: '2023-08-28',
        end: '2023-09-04',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});

it('create-tasks-ads', async () => {
    return createCustomPipelineTasks({
        start: '2023-08-28',
        end: '2023-09-04',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});
