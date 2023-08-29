import * as pipelines from './pipeline.const';
import { createPipelineTasks, runPipeline } from './pipeline.service';

it('pipeline', async () => {
    return runPipeline(pipelines.ADS_INSIGHTS, {
        accountId: '1353175741501928',
        start: '2023-01-01',
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

it('create-tasks', async () => {
    return createPipelineTasks({
        start: '2023-01-01',
        end: '2023-03-01',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            return Promise.reject(error);
        });
});
