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
    [
        "JOB_NAME",
        "DATASET_NAME"
    ]
)

source_file_base = args["DATASET_NAME"]

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

s3 = boto3.client("s3")

BUCKET = "healthcare-proj-data-bkt"

BRONZE_DELTA_BASE_PATH = (
    f"s3://{BUCKET}/delta/bronze/"
)


# Source file name -> Bronze table name
datasets = {

    "NH_ProviderInfo_Oct2024":
        "NH_Provider",

    "PBJ_Daily_Nurse_Staffing_Q2_2024":
        "Staffing",

    "NH_StateUSAverages_Oct2024":
        "NH_StateUSAverage",

    "NH_CovidVaxAverages_20241027":
        "Vaccination"
}


bronze_table = datasets[source_file_base]

# ---------------------------------------------------------
# Manifest path
# ---------------------------------------------------------


def get_manifest_key(
    table_name: str
) -> str:

    return (
        f"metadata/bronze_manifest/"
        f"{table_name}.json"
    )


# ---------------------------------------------------------
# Load manifest
# ---------------------------------------------------------

def load_manifest(
    manifest_key: str
) -> dict:

    try:

        response = s3.get_object(
            Bucket=BUCKET,
            Key=manifest_key
        )

        manifest_data = (
            response["Body"]
            .read()
            .decode("utf-8")
        )

        return json.loads(
            manifest_data
        )

    except ClientError as error:

        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in [
            "NoSuchKey",
            "404"
        ]:

            print(
                "Manifest not found. "
                "Processing as initial load."
            )

            return {}

        raise


# ---------------------------------------------------------
# Save manifest
# ---------------------------------------------------------

def save_manifest(
    manifest: dict,
    manifest_key: str
) -> None:

    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=4
        ).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        f"Manifest saved to "
        f"s3://{BUCKET}/{manifest_key}"
    )


# ---------------------------------------------------------
# Get Google modified time
# ---------------------------------------------------------

def get_google_modified_time(
    file_name: str
) -> str | None:

    source_key = (
        f"raw/{file_name}"
    )

    try:

        response = s3.head_object(
            Bucket=BUCKET,
            Key=source_key
        )

        metadata = response.get(
            "Metadata",
            {}
        )

        return metadata.get(
            "google-modified-time"
        )

    except ClientError as error:

        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in [
            "404",
            "NoSuchKey"
        ]:
            return None

        raise


# ---------------------------------------------------------
# Validate source file is not empty
# ---------------------------------------------------------

def validate_not_empty(
    df: DataFrame,
    dataset_name: str
) -> None:

    if not df.take(1):

        raise ValueError(
            f"No records found for "
            f"{dataset_name}"
        )


# ---------------------------------------------------------
# Add Bronze metadata
# ---------------------------------------------------------

def add_bronze_metadata(
    df: DataFrame,
    source_file_name: str,
    source_modified_time: str,
    batch_id: str
) -> DataFrame:

    source_columns = (
        df.columns
    )

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
            F.lit(
                source_file_name
            )
        )

        .withColumn(
            "source_modified_time",
            F.to_timestamp(
                F.lit(
                    source_modified_time
                )
            )
        )

        .withColumn(
            "batch_id",
            F.lit(
                batch_id
            )
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
# Write Bronze Delta table
# ---------------------------------------------------------

def write_bronze_delta(
    df: DataFrame,
    table_name: str
) -> None:

    output_path = (
        f"{BRONZE_DELTA_BASE_PATH}"
        f"{table_name}/"
    )

    target_exists = (
        DeltaTable.isDeltaTable(
            spark,
            output_path
        )
    )

    if target_exists:

        print(
            f"Appending to existing "
            f"Delta table: "
            f"{output_path}"
        )

        (
            df.write

            .format("delta")

            .mode("append")

            .option(
                "mergeSchema",
                "true"
            )

            .save(
                output_path
            )
        )

    else:

        print(
            f"Creating new Bronze "
            f"Delta table: "
            f"{output_path}"
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

            .save(
                output_path
            )
        )

    print(
        f"{table_name}: Bronze "
        f"Delta write completed"
    )


# ---------------------------------------------------------
# Main processing
# ---------------------------------------------------------

def main() -> None:

    # Example:
    # DATASET_NAME = NH_ProviderInfo_Oct2024

    source_file_base = (
        args["DATASET_NAME"]
    )

    # -----------------------------------------------------
    # Validate Glue parameter
    # -----------------------------------------------------

    if source_file_base not in datasets:

        raise ValueError(
            f"Unknown DATASET_NAME: "
            f"{source_file_base}"
        )

    # -----------------------------------------------------
    # Resolve source and target names
    # -----------------------------------------------------

    bronze_table = (
        datasets[
            source_file_base
        ]
    )

    file_name = (
        f"{source_file_base}.csv"
    )

    input_path = (
        f"s3://{BUCKET}/"
        f"raw/{file_name}"
    )

    manifest_key = (
        get_manifest_key(
            bronze_table
        )
    )

    # -----------------------------------------------------
    # Load this table's manifest
    # -----------------------------------------------------

    manifest = load_manifest(
        manifest_key
    )

    # -----------------------------------------------------
    # Read source file modified timestamp
    # -----------------------------------------------------

    current_modified_time = (
        get_google_modified_time(
            file_name
        )
    )

    if not current_modified_time:

        raise ValueError(
            "No google_modified_time "
            f"metadata found for "
            f"{file_name}"
        )

    previous_modified_time = (
        manifest.get(
            "source_modified_time"
        )
    )

    # -----------------------------------------------------
    # Check whether target already exists
    # -----------------------------------------------------

    output_path = (
        f"{BRONZE_DELTA_BASE_PATH}"
        f"{bronze_table}/"
    )

    target_exists = (
        DeltaTable.isDeltaTable(
            spark,
            output_path
        )
    )

    print(
        f"Source file: "
        f"{file_name}\n"

        f"Bronze table: "
        f"{bronze_table}\n"

        f"Current modified time: "
        f"{current_modified_time}\n"

        f"Previous modified time: "
        f"{previous_modified_time}\n"

        f"Target exists: "
        f"{target_exists}"
    )

    # -----------------------------------------------------
    # Protect against missing manifest
    # -----------------------------------------------------

    if previous_modified_time is None and target_exists:

        raise RuntimeError(
            f"Manifest is missing for {bronze_table}, "
            f"but the Bronze Delta table already exists at "
            f"{output_path}. "
            f"Ingestion stopped to prevent duplicate data."
        )

    # -----------------------------------------------------
    # Skip unchanged source
    # -----------------------------------------------------

    if (
        current_modified_time
        == previous_modified_time
        and target_exists
    ):

        print(
            f"{file_name}: "
            "Source has not changed. "
            "Skipping ingestion."
        )

        return

    # -----------------------------------------------------
    # Explain processing reason
    # -----------------------------------------------------

    if previous_modified_time is None:

        print(
            f"{file_name}: "
            "Initial load."
        )

    elif (
        current_modified_time
        == previous_modified_time
        and not target_exists
    ):

        print(
            f"{file_name}: "
            "Manifest exists but Bronze "
            "table is missing. "
            "Rebuilding table."
        )

    else:

        print(
            f"{file_name}: "
            "Source modified time changed. "
            "Processing new version."
        )

    # -----------------------------------------------------
    # Read source CSV
    # -----------------------------------------------------

    raw_df = (

        spark
        .read

        .option(
            "header",
            "true"
        )

        .option(
            "inferSchema",
            "false"
        )

        .csv(
            input_path
        )
    )

    # -----------------------------------------------------
    # Validate source
    # -----------------------------------------------------

    validate_not_empty(
        raw_df,
        file_name
    )

    # -----------------------------------------------------
    # Create batch ID
    # -----------------------------------------------------

    batch_id = (

        current_modified_time

        .replace("-", "")

        .replace(":", "")

        .replace(".", "")

        .replace("Z", "")

        .replace("+", "")
    )

    # -----------------------------------------------------
    # Add Bronze metadata
    # -----------------------------------------------------

    bronze_df = (
        add_bronze_metadata(
            df=raw_df,
            source_file_name=file_name,
            source_modified_time=(
                current_modified_time
            ),
            batch_id=batch_id
        )
    )

    # -----------------------------------------------------
    # Write Bronze Delta table
    # -----------------------------------------------------

    write_bronze_delta(
        df=bronze_df,
        table_name=bronze_table
    )

    # -----------------------------------------------------
    # Update manifest only after successful write
    # -----------------------------------------------------

    manifest = {

        "source_file_name":
            file_name,

        "source_modified_time":
            current_modified_time,

        "bronze_table":
            bronze_table,

        "batch_id":
            batch_id
    }

    save_manifest(
        manifest=manifest,
        manifest_key=manifest_key
    )

    print(
        f"{bronze_table}: "
        "Processing completed successfully"
    )


# ---------------------------------------------------------
# Job execution
# ---------------------------------------------------------

if __name__ == "__main__":

    try:

        main()

        job.commit()

        print(
            "Bronze Glue job "
            "completed successfully."
        )

    except Exception as error:

        print(
            f"Bronze Glue job failed: "
            f"{error}"
        )

        raise
