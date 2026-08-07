import time
import boto3
import pandas as pd


AWS_REGION = "us-east-2"
ATHENA_DATABASE = "gold"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT_LOCATION = ("s3://healthcare-proj-data-bkt/athena_results/")


def get_athena_client():
    return boto3.client("athena", region_name=AWS_REGION)


def run_athena(query: str, database: str = ATHENA_DATABASE) -> pd.DataFrame:
    athena = get_athena_client()

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_LOCATION
        },
        WorkGroup=ATHENA_WORKGROUP
    )

    query_id = response["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(
            QueryExecutionId=query_id

        )

        status = execution["QueryExecution"]["Status"]["State"]

        print(f"Query Status : {status}")

        if status == 'SUCCEEDED':
            print("Query execution Successfully!!")
            break
        if status in {"FAILED", "CANCELED"}:
            print(f"Query {status}")
            reason = (execution["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown Athena error"))

            raise RuntimeError(f"Athena Query {status} : {reason}")

    time.sleep(2)

    paginator = athena.get_paginator("get_query_results")

    pages = paginator.paginate(QueryExecutionId=query_id)

    rows = []
    column_names = []
    first_page = True

    for page in pages:

        result_set = page["ResultSet"]

        if not column_names:
            column_names = [column["Label"]
                            for column in result_set["ResultSetMetadata"]["ColumnInfo"]]

        page_rows = result_set.get("Rows", [])

        if first_page and page_rows:
            page_rows = page_rows[1:]
            first_page = False

        for row in page_rows:
            data_item = row.get("Data", [])
            value = [item.get("VarCharValue", []) for item in data_item]
            value.extend([None] * (len(column_names) - len(value)))
            rows.append(value)

    return pd.DataFrame(rows, columns=column_names)
