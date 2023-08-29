import * as pipelines from './pipeline.const';
import {
    createAdsPipelineTasks,
    createInsightsPipelineTasks,
    runPipeline,
} from './pipeline.service';

it('pipeline', async () => {
    return runPipeline(pipelines.ADS, {
        accountId: '193539783445588',
        start: '2023-01-01',
        end: '2023-09-01',
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
        start: '2023-01-01',
        end: '2023-03-01',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});

it('create-tasks-ads', async () => {
    return createAdsPipelineTasks({
        start: '2023-01-01',
        end: '2023-03-01',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});
