import {
    ADS_INSIGHTS,
    CAMPAIGNS_PUBLISHER_PLATFORM_INSIGHTS,
    ADS_COUNTRY_REGION_INSIGHTS,
} from './pipeline.const';
import { createPipelineTasks, runPipeline } from './pipeline.service';

it('pipeline', async () => {
    return runPipeline(
        {
            accountId: '1220093882213517',
            start: '2023-04-01',
            end: '2023-05-01',
        },
        ADS_COUNTRY_REGION_INSIGHTS,
    )
        .then((results) => expect(results).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});

it('create-tasks', async () => {
    return createPipelineTasks({
        start: '2023-06-01',
        end: '2023-07-01',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});
