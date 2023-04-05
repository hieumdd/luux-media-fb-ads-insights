import { pipelineService } from './facebook.service';
import { ACCOUNTS, taskService } from './account.service';
import { ADS_INSIGHTS, CAMPAIGNS_PUBLISHER_PLATFORM_INSIGHTS } from './pipeline.const';

describe('pipeline', () => {
    it.concurrent.each(ACCOUNTS)(
        'account %p',
        async (accountId) => {
            console.log(accountId);
            return pipelineService(
                {
                    accountId: String(accountId),
                    start: '2022-06-01',
                    end: '2023-01-01',
                },
                CAMPAIGNS_PUBLISHER_PLATFORM_INSIGHTS,
            ).catch((err) => {
                console.error({ err, accountId });
                return Promise.reject(err);
            });
        },
        540_000,
    );
});

it('task', async () => {
    return taskService({
        start: '2022-03-20',
        end: '2022-04-01',
    }).then((num) => expect(num).toBeGreaterThan(0));
});
