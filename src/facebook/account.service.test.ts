import { getAccounts } from './account.service';
import { BUSINESS_ID } from '../pipeline/pipeline.service';

it('getAccounts', async () => {
    try {
        const results = await getAccounts(BUSINESS_ID);
        expect(results).toBeDefined();
    } catch (error) {
        console.error({ error });
        throw error;
    }
});
