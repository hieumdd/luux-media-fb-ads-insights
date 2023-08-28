import dayjs from '../dayjs';
import { getClient, getExtractStream } from './api.service';
import { PipelineOptions } from '../pipeline/pipeline.request.dto';

type GetDimensionsConfig = {
    endpoint: string;
    fields: string[];
};

export const getDimensionStream = ({ endpoint, fields }: GetDimensionsConfig) => {
    return async ({ accountId, start, end }: PipelineOptions) => {
        const client = await getClient();

        return getExtractStream(client, (after) => ({
            method: 'GET',
            url: `/act_${accountId}/${endpoint}`,
            params: { fields, limit: 250, after },
            filtering: [
                {
                    field: 'ad.updated_time',
                    operator: 'GREATER_THAN_OR_EQUAL',
                    value: dayjs.utc(start).unix(),
                },
                {
                    field: 'ad.updated_time',
                    operator: 'LESS_THAN_OR_EQUAL',
                    value: dayjs.utc(end).unix(),
                },
            ],
        }));
    };
};
