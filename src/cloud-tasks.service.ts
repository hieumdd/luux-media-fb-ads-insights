import { CloudTasksClient, protos } from '@google-cloud/tasks';
import HttpMethod = protos.google.cloud.tasks.v2.HttpMethod;
import { v4 as uuidv4 } from 'uuid';

const client = new CloudTasksClient();

type CreateTasksOptions<P> = {
    location: string;
    queue: string;
    payloads: P[];
    name: (p: P) => string;
};

export const createTasks = async <P>({ location, queue, payloads, name }: CreateTasksOptions<P>) => {
    const [projectId, serviceAccountEmail] = await Promise.all([
        client.getProjectId(),
        client.auth.getCredentials().then((credentials) => credentials.client_email),
    ]);

    const tasks = payloads.map((p) => ({
        parent: client.queuePath(projectId, location, queue),
        task: {
            name: client.taskPath(projectId, location, queue, `${name(p)}-${uuidv4()}`),
            httpRequest: {
                httpMethod: HttpMethod.POST,
                headers: { 'Content-Type': 'application/json' },
                url: process.env.PUBLIC_URL || '',
                oidcToken: { serviceAccountEmail },
                body: Buffer.from(JSON.stringify(p)).toString('base64'),
            },
        },
    }));

    return await Promise.all(tasks.map((r) => client.createTask(r))).then((requests) => requests.length);
};
