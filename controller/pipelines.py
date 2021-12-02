import importlib

from models.AdsInsights.base import AdsInsightsPipeline


def factory(table: str) -> AdsInsightsPipeline:
    try:
        module = importlib.import_module(f"models.AdsInsights.{table}")
        return getattr(module, table)
    except (ImportError, AttributeError):
        raise ValueError(table)


def run(pipeline: AdsInsightsPipeline, request_data: dict) -> dict:
    return pipeline(
        request_data["ads_account_id"],
        request_data["start"],
        request_data["end"],
    )
