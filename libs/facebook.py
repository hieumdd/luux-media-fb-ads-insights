from typing import Optional, TypedDict
import os
import json
from datetime import datetime
import time

import requests

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

API_VER = "v12.0"
BASE_URL = f"https://graph.facebook.com/{API_VER}/"


class AsyncFailedException(Exception):
    def __init__(self, message):
        super().__init__(f"Async Job Failed: {message}")


class RequestOptions(TypedDict):
    level: str
    fields: list[str]
    breakdowns: Optional[str]


ReportRunId = str
Insights = list[dict]


def _request_async_report(
    request_options: RequestOptions,
    session: requests.Session,
    ads_account_id: str,
    start: datetime,
    end: datetime,
) -> ReportRunId:
    params = {
        "access_token": os.getenv("ACCESS_TOKEN"),
        "level": request_options["level"],
        "fields": json.dumps(request_options["fields"]),
        "action_attribution_windows": json.dumps(
            [
                "1d_click",
                "1d_view",
                "7d_click",
                "7d_view",
            ]
        ),
        "filtering": json.dumps(
            [
                {
                    "field": "ad.impressions",
                    "operator": "GREATER_THAN",
                    "value": 0,
                },
                {
                    "field": "ad.effective_status",
                    "operator": "IN",
                    "value": [
                        "ACTIVE",
                        "PAUSED",
                        "DELETED",
                        "PENDING_REVIEW",
                        "DISAPPROVED",
                        "PREAPPROVED",
                        "PENDING_BILLING_INFO",
                        "CAMPAIGN_PAUSED",
                        "ARCHIVED",
                        "ADSET_PAUSED",
                        "IN_PROCESS",
                        "WITH_ISSUES",
                    ],
                },
            ]
        ),
        "time_increment": 1,
        "time_range": json.dumps(
            {
                "since": start.strftime(DATE_FORMAT),
                "until": end.strftime(DATE_FORMAT),
            }
        ),
    }
    if request_options.get("breakdowns"):
        params["breakdowns"] = request_options["breakdowns"]
    with session.post(
        f"{BASE_URL}/act_{ads_account_id}/insights",
        params=params,
    ) as r:
        res = r.json()
    return res["report_run_id"]


def _poll_async_report(
    session: requests.Session,
    report_run_id: ReportRunId,
) -> ReportRunId:
    with session.get(
        f"{BASE_URL}/{report_run_id}",
        params={"access_token": os.getenv("ACCESS_TOKEN")},
    ) as r:
        res = r.json()
    if (
        res["async_percent_completion"] == 100
        and res["async_status"] == "Job Completed"
    ):
        return report_run_id
    elif res["async_status"] == "Job Failed":
        raise AsyncFailedException(report_run_id)
    else:
        time.sleep(5)
        return _poll_async_report(session, report_run_id)


def _get_async_report(
    request_options: RequestOptions,
    session: requests.Session,
    ads_account_id: str,
    start: datetime,
    end: datetime,
    attempt: int = 0,
) -> ReportRunId:
    report_run_id = _request_async_report(
        request_options,
        session,
        ads_account_id,
        start,
        end,
    )
    try:
        return _poll_async_report(session, report_run_id)
    except AsyncFailedException as e:
        if attempt < 5:
            return _get_async_report(
                request_options,
                session,
                ads_account_id,
                start,
                end,
                attempt + 1,
            )
        else:
            raise e


def _get_insights(
    session: requests.Session,
    report_run_id: ReportRunId,
    after: str = None,
) -> Insights:
    try:
        with session.get(
            f"{BASE_URL}/{report_run_id}/insights",
            params={
                "access_token": os.getenv("ACCESS_TOKEN"),
                "limit": 500,
                "after": after,
            },
        ) as r:
            res = r.json()
        data = res["data"]
        next_ = res["paging"].get("next")
        return (
            data
            + _get_insights(session, report_run_id, res["paging"]["cursors"]["after"])
            if next_
            else data
        )
    except KeyError:
        return _get_insights(session, report_run_id, after)


def get(
    request_options: RequestOptions,
    ads_account_id: str,
    start: datetime,
    end: datetime,
) -> Insights:
    with requests.Session() as session:
        return _get_insights(
            session,
            _get_async_report(
                request_options,
                session,
                ads_account_id,
                start,
                end,
            ),
        )
