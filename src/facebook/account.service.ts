import { createTasks } from '../task/cloud-tasks.service';
import * as pipelines from './pipeline.const';

export const ACCOUNTS = [
    '892630467592850',
    '259707128315398',
    '1353175741501928',
    '2420986814809109',
    '740269656424403',
];

export type TaskOptions = {
    start?: string;
    end?: string;
};

export const taskService = ({ start, end }: TaskOptions) => {
    const data = Object.values(ACCOUNTS).flatMap((accountId) => {
        return [pipelines.ADS_INSIGHTS, pipelines.CAMPAIGNS_PUBLISHER_PLATFORM_INSIGHTS].map(
            (pipeline) => ({
                accountId: String(accountId),
                start,
                end,
                pipeline,
            }),
        );
    });

    return createTasks(data, (task) => task.accountId);
};
