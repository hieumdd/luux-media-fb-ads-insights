import os
import json
import uuid

from google.cloud import tasks_v2
from google import auth

_, PROJECT_ID = auth.default()
TASKS_CLIENT = tasks_v2.CloudTasksClient()

TABLES = [
    "AdsInsights",
    # "VideoInsights",
    # "AgeGenderInsights",
    # "RegionInsights",
    # "DeviceInsights",
    # "PlatformPositionInsights",
]

ACCOUNTS = [
    "892630467592850",
    "259707128315398",
]

CLOUD_TASKS_PATH = (PROJECT_ID, "us-central1", "fb-ads-insights")
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)


def create_tasks(tasks_data: dict) -> dict:
    payloads = [
        {
            "name": f"{account}-{uuid.uuid4()}",
            "payload": {
                "table": tasks_data["table"],
                "ads_account_id": account,
                "start": tasks_data.get("start"),
                "end": tasks_data.get("end"),
            },
        }
        for account in ACCOUNTS
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(
                *CLOUD_TASKS_PATH,
                task=str(payload["name"]),
            ),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": os.getenv("PUBLIC_URL"),
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {"Content-type": "application/json"},
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    return {
        "tasks": len(
            [
                TASKS_CLIENT.create_task(
                    request={
                        "parent": PARENT,
                        "task": task,
                    }
                )
                for task in tasks
            ]
        ),
        "tasks_data": tasks_data,
    }
