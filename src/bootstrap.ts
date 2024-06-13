import { getLogger } from './logging.service';
import * as pipelines from './pipeline/pipeline.const';
import { BUCKET, DATASET } from './pipeline/pipeline.service';

const logger = getLogger(__filename);

(async () => {
    await Promise.all(
        Object.values(pipelines).map((pipeline) => pipeline.bootstrap({ bucket: BUCKET, dataset: DATASET })),
    );
    const accountTable = DATASET.table('Accounts');
    if (await accountTable.exists().then(([results]) => results)) {
        await accountTable.delete();
    }

    await accountTable.create({
        schema: [
            { name: 'account_id', type: 'INT64' },
            { name: 'account_name', type: 'STRING' },
        ],
        externalDataConfiguration: {
            sourceUris: [`gs://${BUCKET.name}/Accounts.json`],
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            ignoreUnknownValues: true,
        },
    });
})()
    .then(() => {
        logger.info('Bootstrap successfully');
        process.exit(0);
    })
    .catch((error) => {
        logger.error('Bootstrap failed', { error });
        process.exit(1);
    });
