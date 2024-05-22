import * as pipelines from './pipeline.const';
import { createCustomPipelineTasks, createPipelineTasks, runPipeline } from './pipeline.service';

describe('pipeline', () => {
    it.each([
        pipelines.CampaignsDevicePlatformPositionInsights,
        pipelines.CampaignsCountryInsights,
        // pipelines.Ads,
        // pipelines.CampaignsAgeGenderInsights,
        // pipelines.CampaignsRegionInsights,
    ])(
        '$.name',
        async (pipeline) => {
            try {
                const results = await runPipeline(pipeline, {
                    accountId: '1353175741501928',
                    start: '2024-01-01',
                    end: '2024-01-15',
                });
                expect(results).toBeDefined();
            } catch (error) {
                console.error({ error });
                throw error;
            }
        },
        100_000_000,
    );
});

describe('tasks', () => {
    const options = { start: '2023-08-28', end: '2023-09-04' };

    it('createPipelineTasks', async () => {
        try {
            const results = await createPipelineTasks(options);
            expect(results).toBeDefined();
        } catch (error) {
            console.error({ error });
            throw error;
        }
    });

    it('createCustomPipelineTasks', async () => {
        try {
            const results = await createCustomPipelineTasks(options);
            expect(results).toBeDefined();
        } catch (error) {
            console.error({ error });
            throw error;
        }
    });
});
