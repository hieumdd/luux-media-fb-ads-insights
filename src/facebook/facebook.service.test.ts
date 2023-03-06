import { pipelineService } from './facebook.service';
import { ACCOUNTS, taskService } from './account.service';
import { ADS_INSIGHTS } from './pipeline.const';

describe('pipeline', () => {
    it.concurrent.each(ACCOUNTS)(
        'account %p',
        async (accountId) => {
            console.log(accountId);
            return pipelineService(
                {
                    accountId: String(accountId),
                    start: '2023-01-01',
                    end: '2023-03-01',
                },
                ADS_INSIGHTS,
            ).catch((err) => {
                console.error({ err, accountId });
                return Promise.reject(err);
            });
        },
        540_000,
    );
});

it('Task Service', async () => {
    return taskService({
        pipeline: 'ADS_INSIGHTS',
        start: '2022-02-01',
        end: '2022-02-07',
    }).then((num) => expect(num).toBeGreaterThan(0));
});
