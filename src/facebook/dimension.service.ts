import { getClient, getExtractStream } from './api.service';
import { PipelineOptions } from '../pipeline/pipeline.request.dto';

type GetDimensionsConfig = {
    endpoint: string;
    fields: string[];
};

export const getDimensionStream = ({ endpoint, fields }: GetDimensionsConfig) => {
    return async (options: PipelineOptions) => {
        const client = await getClient();

        return getExtractStream(client, (after) => ({
            method: 'GET',
            url: `/${options.accountId}/${endpoint}`,
            params: { fields, limit: 500, after },
        }));
    };
};
