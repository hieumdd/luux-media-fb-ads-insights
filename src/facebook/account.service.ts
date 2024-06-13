import path from 'node:path';

import { getClient } from './api.service';

export const getAccounts = async (businessId: string) => {
    const client = await getClient();

    const get = async (edge: string) => {
        type Response = { data: { account_id: string; name: string }[] };
        const { data } = await client.request<Response>({
            method: 'GET',
            url: path.join('/', businessId, edge),
            params: { limit: 500, fields: ['name', 'account_id'] },
        });
        return data.data.map(({ account_id, name }) => ({ account_id, account_name: name }));
    };

    const results = await Promise.all([get('client_ad_accounts'), get('owned_ad_accounts')]);
    return results.flat();
};
