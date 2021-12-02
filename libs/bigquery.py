from typing import TypedDict

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()


class LoadOptions(TypedDict):
    name: str
    schema: list[dict]
    p_key: list[str]


def load(
    load_options: LoadOptions,
    dataset: str,
    ads_account_id: str,
    rows: list[dict],
) -> int:
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{dataset}.{load_options['name']}_{ads_account_id}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=load_options["schema"],
            ),
        )
        .result()
        .output_rows
    )
    update(load_options, dataset, ads_account_id)
    return output_rows


def update(load_options: LoadOptions, dataset: str, ads_account_id: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{load_options['name']}_{ads_account_id} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {','.join(load_options['p_key'])}
            ORDER BY _batched_at DESC) AS row_num
        FROM {dataset}.{load_options['name']}_{ads_account_id}
    ) WHERE row_num = 1"""
    ).result()
