import dayjs from 'dayjs';
import Joi from 'joi';

import { Pipeline } from './pipeline.const';
import { get } from './facebook.repository';
import { load } from '../bigquery/bigquery.service';

export type PipelineOptions = {
    accountId: string;
    start?: string;
    end?: string;
};

export const pipelineService = async (options: PipelineOptions, pipeline: Pipeline) => {
    const [start, end] = [
        options.start ? dayjs(options.start) : dayjs().subtract(7, 'days'),
        options.end ? dayjs(options.end) : dayjs(),
    ];

    return get({
        ...pipeline.insightsOptions,
        accountId: options.accountId,
        start,
        end,
    })
        .then((rows) => rows.map((row) => Joi.attempt(row, pipeline.validationSchema)))
        .then((data) => {
            const table = `${pipeline.name}__${options.accountId}`;
            return load(data, { table, schema: pipeline.schema });
        });
};
