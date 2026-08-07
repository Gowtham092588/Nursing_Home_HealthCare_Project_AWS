import sys
import json
import boto3

from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# ---------------------------------------------------------
# Glue job initialization
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

s3 = boto3.client("s3")

BUCKET = "healthcare-proj-data-bkt"

MANIFEST_KEY = "metadata/bronze_delta_manifest.json"

BRONZE_DELTA_BASE_PATH = (
    f"s3://{BUCKET}/delta/bronze/"
)


datasets = {
    "NH_ProviderInfo_Oct2024": "NH_Provider",
    "PBJ_Daily_Nurse_Staffing_Q2_2024": "Staffing",
    "NH_StateUSAverages_Oct2024": "NH_StateUSAverage",
    "NH_CovidVaxAverages_20241027": "Vaccination"
}


# ---------------------------------------------------------
# Manifest functions
# ---------------------------------------------------------

def load_manifest() -> dict:

    try:
        response = s3.get_object(
            Bucket=BUCKET,
            Key=MANIFEST_KEY
        )

        return json.loads(
            response["Body"]
            .read()
            .decode("utf-8")
        )

    except ClientError as error:
        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in [
            "NoSuchKey",
            "404",
            "NoSuchBucket"
        ]:
            print(
                "Manifest not found. "
                "A new manifest will be created."
            )

            return {}

        raise


def save_manifest(manifest: dict) -> None:
    """
    Save the processing manifest to S3.
    """

    s3.put_object(
        Bucket=BUCKET,
        Key=MANIFEST_KEY,
        Body=json.dumps(
            manifest,
            indent=4
        ).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        f"Manifest saved to "
        f"s3://{BUCKET}/{MANIFEST_KEY}"
    )


# ---------------------------------------------------------
# Source-file metadata
# ---------------------------------------------------------

def get_google_modified_time(file_name: str) -> str | None:
    """
    Read google_modified_time from the source S3 object's
    custom metadata.
    """

    source_key = f"raw/{file_name}"

    try:
        response = s3.head_object(
            Bucket=BUCKET,
            Key=source_key
        )

        metadata = response.get("Metadata", {})

        return metadata.get("google-modified-time")

    except ClientError as error:
        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in ["404", "NoSuchKey"]:
            return None
        raise

# ---------------------------------------------------------
# Data validation
# ---------------------------------------------------------


def validate_not_empty(df, dataset_name: str) -> None:
    """
    Stop the job when a source file contains no records.
    """

    if not df.take(1):
        raise ValueError(
            f"No records found for {dataset_name}"
        )


def add_bronze_metadata(df: DataFrame, source_file_name: str, source_modified_time: str, batch_id: str):
    """
    Add technical Bronze-layer metadata.
    """

    source_columns = df.columns

    hash_columns = [
        F.coalesce(
            F.col(column).cast("string"),
            F.lit("__NULL__")
        )
        for column in source_columns
    ]

    return (
        df
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.input_file_name()
        )
        .withColumn(
            "source_file_name",
            F.lit(source_file_name)
        )
        .withColumn(
            "source_modified_time",
            F.to_timestamp(
                F.lit(source_modified_time)
            )
        )
        .withColumn(
            "batch_id",
            F.lit(batch_id)
        )
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *hash_columns
                ),
                256
            )
        )
    )


# ---------------------------------------------------------
# Delta writer
# ---------------------------------------------------------

def write_bronze_delta(df, table_name: str) -> None:
    output_path = (
        f"{BRONZE_DELTA_BASE_PATH}"
        f"{table_name}/"
    )

    if DeltaTable.isDeltaTable(
        spark,
        output_path
    ):
        print(
            f"Appending to existing Delta table: "
            f"{output_path}"
        )

        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(output_path)
        )

    else:
        print(
            f"Creating new Delta table with "
            f"column mapping: {output_path}"
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "delta.columnMapping.mode",
                "name"
            )
            .option(
                "delta.minReaderVersion",
                "2"
            )
            .option(
                "delta.minWriterVersion",
                "5"
            )
            .option(
                "mergeSchema",
                "true"
            )
            .save(output_path)
        )

    print(
        f"Successfully wrote Bronze Delta table "
        f"{table_name}"
    )


# ---------------------------------------------------------
# Main processing
# ---------------------------------------------------------


def main() -> None:
    manifest = load_manifest()

    for source_file_base, bronze_table in datasets.items():

        file_name = (f"{source_file_base}.csv")

        input_path = (f"s3://{BUCKET}/raw/{file_name}")

        current_modified_time = (get_google_modified_time(file_name))

        if not current_modified_time:
            raise ValueError(
                "No google_modified_time metadata "
                f"found for {file_name}"
            )

        previous_modified_time = (manifest.get(file_name))

        previous_modified_time = manifest.get(file_name)

        output_path = (
            f"{BRONZE_DELTA_BASE_PATH}"
            f"{bronze_table}/"
        )

        target_exists = DeltaTable.isDeltaTable(
            spark,
            output_path
        )

        print(
            f"File: {file_name}\n"
            f"Bronze table: {bronze_table}\n"
            f"Current Google modified time: "
            f"{current_modified_time}\n"
            f"Previous manifest time: "
            f"{previous_modified_time}\n"
            f"Target Delta path: {output_path}\n"
            f"Target Delta table exists: {target_exists}"
        )

        if (
            current_modified_time == previous_modified_time
            and target_exists
        ):
            print(
                f"{file_name}: Timestamp has not changed "
                "and the Bronze Delta table exists. Skipping."
            )
            continue

        if previous_modified_time is None:
            print(
                f"{file_name}: No manifest entry found. "
                "Processing as an initial load."
            )

        elif (
            current_modified_time == previous_modified_time
            and not target_exists
        ):
            print(
                f"{file_name}: Timestamp has not changed, "
                "but the Bronze Delta table is missing. "
                "Reprocessing to rebuild the table."
            )

        else:
            print(
                f"{file_name}: Google Drive modified time "
                "changed. Processing the latest file."
            )

        raw_df = (
            spark
            .read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(input_path)
        )

        validate_not_empty(raw_df, file_name)

        batch_id = (
            current_modified_time
            .replace("-", "")
            .replace(":", "")
            .replace(".", "")
            .replace("Z", "")
            .replace("+", "")
        )

        bronze_df = add_bronze_metadata(
            df=raw_df,
            source_file_name=file_name,
            source_modified_time=(current_modified_time),
            batch_id=batch_id
        )

        write_bronze_delta(
            df=bronze_df,
            table_name=bronze_table
        )

        # Update manifest only after the Delta write succeeds.
        manifest[file_name] = current_modified_time

        # Save after each table so completed tables are not
        # loaded again if a later dataset fails.
        save_manifest(manifest)

        print(
            f"Completed {bronze_table}"
        )

# ---------------------------------------------------------
# Job execution
# ---------------------------------------------------------


if __name__ == "__main__":
    try:
        main()

        print(
            "Bronze Delta ingestion job completed successfully."
        )

    except Exception as error:
        print(f"Glue job failed: {str(error)}")

        raise

    finally:
        job.commit()
