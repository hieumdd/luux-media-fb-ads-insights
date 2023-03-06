import Joi from 'joi';
import { http } from '@google-cloud/functions-framework';
import express from 'express';

import * as pipelines from './facebook/pipeline.const';
import { pipelineService } from './facebook/facebook.service';
import { taskService } from './facebook/account.service';

const app = express();

type RunPipelineDto = {
    pipeline: keyof typeof pipelines;
    accountId: string;
    start?: string;
    end?: string;
};

app.use(({ path, body }, res, next) => {
    const log = { path, body };
    console.log(JSON.stringify(log));
    next();
});

app.use('/task', (req, res) => {
    Joi.object<Omit<RunPipelineDto, 'accountId'>>({
        pipeline: Joi.string(),
        start: Joi.string().optional(),
        end: Joi.string().optional(),
    })
        .validateAsync(req.body)
        .then((body) =>
            taskService(body).then((result) => {
                res.status(200).json({ result });
            }),
        )
        .catch((err) => {
            console.error('err', err);
            res.status(500).json({ err });
        });
});

app.use('/', (req, res) => {
    Joi.object<RunPipelineDto>({
        pipeline: Joi.string(),
        accountId: Joi.string(),
        start: Joi.string().optional(),
        end: Joi.string().optional(),
    })
        .validateAsync(req.body)
        .then(({ pipeline, accountId, start, end }) =>
            pipelineService({ accountId, start, end }, pipelines[pipeline]),
        )
        .then((result) => res.status(200).json({ result }))
        .catch((err) => {
            console.error('err', err);
            res.status(500).json({ err });
        });
});

http('main', app);
