import path from 'node:path';
import { setTimeout } from 'node:timers/promises';

import { getLogger } from '../logging.service';
import { getClient, getPaginatedStream } from './api.service';
import { FacebookRequestOptions } from '../pipeline/pipeline.request.dto';

const logger = getLogger(__filename);

export type GetInsightsConfig = {
    level: string;
    fields: string[];
    breakdowns?: string;
};

export const getInsightsStream = (config: GetInsightsConfig) => {
    return async (options: FacebookRequestOptions) => {
        const client = await getClient();

        const requestReport = async (): Promise<string> => {
            const { accountId, start: since, end: until } = options;
            const { level, fields, breakdowns } = config;
            const { data } = await client.request<{ report_run_id: string }>({
                method: 'POST',
                url: path.join('/', `act_${accountId}`, 'insights'),
                data: {
                    level,
                    fields,
                    breakdowns,
                    time_range: JSON.stringify({ since, until }),
                    time_increment: 1,
                },
            });
            return data.report_run_id;
        };

        const pollReport = async (reportId: string, attempt = 1): Promise<string> => {
            type Response = { async_percent_completion: number; async_status: string };
            const { data } = await client.request<Response>({ method: 'GET', url: `/${reportId}` });

            if (data.async_percent_completion === 100 && data.async_status === 'Job Completed') {
                return reportId;
            }

            if (data.async_status === 'Job Failed') {
                logger.error('Facebook async job failed', data);
                throw new Error(data.async_status);
            }

            if (attempt > 5) {
                logger.error('Facebook async job timeout', data);
                throw new Error('Job Timeout');
            }

            await setTimeout(2 * attempt * 10 * 10000);
            return await pollReport(reportId);
        };

        const reportId = await requestReport().then(pollReport);
        return getPaginatedStream(client, (after) => ({
            method: 'GET',
            url: `/${reportId}/insights`,
            params: { after, limit: 500 },
        }));
    };
};
