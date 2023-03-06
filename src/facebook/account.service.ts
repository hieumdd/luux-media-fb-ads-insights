import { createTasks } from '../task/cloud-tasks.service';
import * as pipelines from './pipeline.const';

export const ACCOUNTS = ['892630467592850', '259707128315398'];

export type TaskOptions = {
    pipeline: keyof typeof pipelines;
    start?: string;
    end?: string;
};

export const taskService = ({ pipeline, start, end }: TaskOptions) => {
    const data = Object.values(ACCOUNTS).map((accountId) => ({
        accountId: String(accountId),
        start,
        end,
        pipeline,
    }));

    return createTasks(data, (task) => task.accountId);
};
