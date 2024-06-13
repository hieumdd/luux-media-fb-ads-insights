import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { Dataset } from '@google-cloud/bigquery';
import { Bucket } from '@google-cloud/storage';
import ndjson from 'ndjson';
import { NdJson } from 'json-nd';
import Joi from 'joi';
const PQueue = require('@esm2cjs/p-queue').default;

import { getLogger } from '../logging.service';
import { dayjs } from '../dayjs';
import { FacebookRequestOptions } from './pipeline.request.dto';
import { Field } from './pipeline.schema';

const logger = getLogger(__filename);

const transformValidate = (schema: Joi.Schema) => {
    return async function* (rows: any) {
        for await (const row of rows) {
            const value = await schema.validateAsync(row);
            yield { ...value, _batched_at: dayjs().utc().toISOString() };
        }
    };
};

export type RunPipelineOptions = FacebookRequestOptions & { bucket: Bucket };

type CreatePipelineConfig = {
    name: string;
    extractStream: (options: FacebookRequestOptions) => Promise<Readable>;
    fields: Field[];
};

export const createInsightsPipeline = (options: CreatePipelineConfig) => {
    const { name, extractStream, fields } = options;
    const validationSchema = Joi.object(Object.assign({}, ...fields.map((field) => field.validationSchema)));
    const tableSchema = fields.map((field) => field.tableSchema);
    const format = 'json';
    const storageConfig = {
        path: (accountId: string, key: string) => `${name}/_account_id=${accountId}/_date_start=${key}/data.json`,
        sourceUris: (bucketName: string) => [`gs://${bucketName}/${name}/*.${format}`],
        sourceUriPrefix: (bucketName: string) => `gs://${bucketName}/${name}/{_account_id:INT64}/{_date_start:DATE}`,
    };

    const transformGroup = async function* (rows: any[]) {
        const grouped: Record<string, object[]> = {};
        for await (const row of rows) {
            grouped[row.date_start] = [...(grouped[row.date_start] ?? []), row];
        }
        for (const group of Object.entries(grouped)) {
            yield group;
        }
    };

    const execute = async (options: RunPipelineOptions) => {
        const sourceStream = await extractStream(options);

        const writeStream = async function (files: [string, any][]) {
            const queue = new PQueue();
            for await (const [key, data] of files) {
                queue.add(() => {
                    return options.bucket
                        .file(storageConfig.path(options.accountId, key))
                        .save(NdJson.stringify(data), { resumable: false });
                });
            }
            await queue.onIdle();
        };
        // @ts-expect-error
        await pipeline(sourceStream, transformValidate(validationSchema), transformGroup, writeStream);
        return true;
    };

    const bootstrap = async ({ bucket, dataset }: { bucket: Bucket; dataset: Dataset }) => {
        const table = dataset.table(`${name}`);
        const tableName = `${table.dataset.id}.${table.id}`;

        if (await table.exists().then(([response]) => response)) {
            logger.debug(`Replacing table ${tableName}`);
            await table.delete();
        }

        await table.create({
            schema: tableSchema,
            externalDataConfiguration: {
                sourceUris: storageConfig.sourceUris(bucket.name),
                sourceFormat: 'NEWLINE_DELIMITED_JSON',
                ignoreUnknownValues: true,
                hivePartitioningOptions: {
                    mode: 'CUSTOM',
                    sourceUriPrefix: storageConfig.sourceUriPrefix(bucket.name),
                },
            },
        });
        logger.debug(`Table ${tableName} created`);
    };

    return { name, execute, bootstrap };
};

export const createDimensionsPipeline = (options: CreatePipelineConfig) => {
    const { name, extractStream, fields } = options;
    const validationSchema = Joi.object(Object.assign({}, ...fields.map((field) => field.validationSchema)));
    const tableSchema = fields.map((field) => field.tableSchema);
    const format = 'json';
    const storageConfig = {
        path: (accountId: string) => `${name}/_account_id=${accountId}/data.json`,
        sourceUris: (bucketName: string) => [`gs://${bucketName}/${name}/*.${format}`],
        sourceUriPrefix: (bucketName: string) => `gs://${bucketName}/${name}/{_account_id:INT64}`,
    };

    const execute = async (options: RunPipelineOptions) => {
        const sourceStream = await extractStream(options);

        await pipeline(
            sourceStream,
            transformValidate(validationSchema),
            ndjson.stringify(),
            options.bucket.file(storageConfig.path(options.accountId)).createWriteStream({ resumable: false }),
        );
        return true;
    };

    const bootstrap = async ({ bucket, dataset }: { bucket: Bucket; dataset: Dataset }) => {
        const table = dataset.table(`${name}`);
        const tableName = `${table.dataset.id}.${table.id}`;

        if (await table.exists().then(([response]) => response)) {
            logger.debug(`Replacing table ${tableName}`);
            await table.delete();
        }

        await table.create({
            schema: tableSchema,
            externalDataConfiguration: {
                sourceUris: storageConfig.sourceUris(bucket.name),
                sourceFormat: 'NEWLINE_DELIMITED_JSON',
                ignoreUnknownValues: true,
            },
        });
        logger.debug(`Table ${tableName} created`);
    };

    return { name, execute, bootstrap };
};

export type Pipeline = ReturnType<typeof createInsightsPipeline | typeof createDimensionsPipeline>;
