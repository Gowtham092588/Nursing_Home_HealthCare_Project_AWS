import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    DateType,
    DecimalType,
    FloatType
)
from datetime import datetime, timezone
from pyspark.sql.window import Window
from delta.tables import DeltaTable

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TABLE_NAME'])

table_name = args["TABLE_NAME"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3 = boto3.client("s3")
BRONZE_BUCKET = "healthcare-proj-data-bkt"
SILVER_BUCKET = "healthcare-proj-data-bkt"
QUARANTINE_BUCKET = "healthcare-proj-data-bkt"


def load_config_json(config_uri: str) -> dict:
    path = config_uri.replace("s3://", "", 1)
    bucket, key = path.split("/", 1)

    response = s3.get_object(
        Bucket=bucket,
        Key=key)

    return json.loads(response['Body'].read().decode("utf-8"))


def read_bronze_table(spark, bronze_bucket: str, table_config: dict):

    source_path = (
        f"s3://{bronze_bucket}/"
        f"{table_config['source_path'].strip('/')}/"
    )

    print(
        f"Reading Bronze Delta table from "
        f"{source_path}"
    )

    return (
        spark
        .read
        .format("delta")
        .load(source_path)
    )


def select_source_snapshot(df: DataFrame, table_name: str, table_config: dict) -> DataFrame:

    snapshot_config = table_config.get("source_snapshot", {})

    if not snapshot_config.get("enabled", False):
        print(f"{table_name}: Source snapshot filtering is disabled")
        return df

    order_column = snapshot_config.get("order_column", "source_modified_time")
    selection_method = snapshot_config.get("select", "latest").lower()

    if order_column not in df.columns:
        raise ValueError(f"{table_name}: Snapshot order column "
                         f"'{order_column}' does not exist"
                         )

    if selection_method != "latest":
        raise ValueError(
            f"{table_name}: Unsupported snapshot "
            f"selection method '{selection_method}'"
        )

    latest_value = (df.select(F.max(F.col(order_column)).alias(
        "latest_snapshot")).first()["latest_snapshot"])

    if latest_value is None:
        raise ValueError(
            f"{table_name}: No valid value found in "
            f"snapshot column '{order_column}'"
        )

    print(
        f"{table_name}: Selecting snapshot where "
        f"{order_column} = {latest_value}"
    )

    return df.filter(F.col(order_column) == F.lit(latest_value))


def add_scd2_record_hash(df: DataFrame, table_name: str, scd2_config: dict) -> DataFrame:

    tracked_columns = scd2_config.get("tracked_columns", [])

    if not tracked_columns:
        raise ValueError(
            f"{table_name}: No tracked_columns "
            "configured for SCD2"
        )

    missing_columns = [
        column_name
        for column_name in tracked_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name}: SCD2 tracked columns "
            f"do not exist: {missing_columns}"
        )

    columns_config = scd2_config.get("columns", {})
    hash_column = columns_config.get("record_hash", "scd_record_hash")

    normalized_values = [
        F.coalesce(
            F.trim(
                F.col(column_name).cast("string")
            ),
            F.lit("<NULL>")
        )
        for column_name in tracked_columns
    ]

    return df.withColumn(
        hash_column,
        F.sha2(
            F.concat_ws(
                "||",
                *normalized_values
            ),
            256
        )
    )


def validate_scd2_config(df: DataFrame, table_name: str, table_config: dict) -> None:

    scd2_config = table_config.get("scd2", {})

    if not scd2_config.get("enabled", False):
        raise ValueError(
            f"{table_name}: load_type is SCD2, "
            "but scd2.enabled is false"
        )

    business_keys = scd2_config.get("business_keys", [])

    if not business_keys:
        raise ValueError(
            f"{table_name}: SCD2 business_keys "
            "cannot be empty"
        )

    effective_date_column = scd2_config.get("effective_date_column")

    if not effective_date_column:
        raise ValueError(
            f"{table_name}: SCD2 "
            "effective_date_column is required"
        )

    required_scd_columns = (
        business_keys
        + scd2_config.get("tracked_columns", [])
        + [effective_date_column]
    )

    missing_columns = [
        column_name
        for column_name in required_scd_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name}: Required SCD2 columns "
            f"are missing: {missing_columns}"
        )


def prepare_scd2_source(df: DataFrame, table_name: str, table_config: dict) -> DataFrame:

    scd2_config = table_config.get("scd2", {})

    validate_scd2_config(
        df=df,
        table_name=table_name,
        table_config=table_config
    )

    df = add_scd2_record_hash(
        df=df,
        table_name=table_name,
        scd2_config=scd2_config
    )

    columns_config = scd2_config.get("columns", {})
    effective_date_column = scd2_config.get("effective_date_column")
    start_column = columns_config.get(
        "effective_start_date", "effective_start_date")
    end_column = columns_config.get("effective_end_date", "effective_end_date")
    current_column = columns_config.get("current_flag", "is_current")
    insert_timestamp_column = columns_config.get(
        "insert_timestamp", "scd_insert_timestamp")
    update_timestamp_column = columns_config.get(
        "update_timestamp", "scd_update_timestamp")
    open_end_date = scd2_config.get("open_end_date", "9999-12-31")

    return (
        df
        .withColumn(
            start_column,
            F.col(effective_date_column).cast("date")
        )
        .withColumn(
            end_column,
            F.to_date(
                F.lit(open_end_date),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            current_column,
            F.lit(True)
        )
        .withColumn(
            insert_timestamp_column,
            F.current_timestamp()
        )
        .withColumn(
            update_timestamp_column,
            F.lit(None).cast("timestamp")
        )
    )


def build_key_condition(business_keys: list, target_alias: str = "target", source_alias: str = "source") -> str:

    if not business_keys:
        raise ValueError(
            "At least one business key is required"
        )

    return " AND ".join(
        [
            (
                f"{target_alias}.`{column_name}` "
                f"<=> "
                f"{source_alias}.`{column_name}`"
            )
            for column_name in business_keys
        ]
    )


def columns_renaming(df: DataFrame, rename_columns: dict[str, str]) -> DataFrame:

    for source_column, target_column in rename_columns.items():
        if source_column not in df.columns:
            raise ValueError(f"Source column {source_column} does not exist")
        df = df.withColumnRenamed(source_column, target_column)

    return df


def clean_string_columns(df: DataFrame) -> DataFrame:

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            cleaned_value = F.col(field.name)

            # Replace non-breaking spaces with normal spaces
            cleaned_value = F.regexp_replace(
                cleaned_value,
                "\u00A0",
                " "
            )

            # Remove leading/trailing spaces, tabs,
            # newlines and carriage returns
            cleaned_value = F.regexp_replace(
                cleaned_value,
                r"^[\s]+|[\s]+$",
                ""
            )

            # Convert doubled CSV quotes "" -> "
            cleaned_value = F.regexp_replace(
                cleaned_value,
                '""',
                '"'
            )

            # Remove only outer wrapping quotes
            cleaned_value = F.regexp_replace(
                cleaned_value,
                '^"(.*)"$',
                '$1'
            )

            # Convert blank / NULL text to actual NULL
            df = df.withColumn(
                field.name,
                F.when(
                    cleaned_value.isNull()
                    | (cleaned_value == "")
                    | (F.upper(cleaned_value) == "NULL"),
                    F.lit(None).cast("string")
                ).otherwise(cleaned_value)
            )

    return df


def round_numeric_columns(df: DataFrame, scale: int = 2) -> DataFrame:

    for field in df.schema.fields:

        column_name = field.name
        data_type = field.dataType

        if isinstance(data_type, T.DecimalType):

            df = df.withColumn(
                column_name,
                F.round(
                    F.col(column_name),
                    scale
                ).cast(
                    T.DecimalType(
                        data_type.precision,
                        scale
                    )
                )
            )

        elif isinstance(
            data_type,
            (T.DoubleType, T.FloatType)
        ):

            df = df.withColumn(
                column_name,
                F.round(
                    F.col(column_name),
                    scale
                ).cast(data_type)
            )

    return df


def title_case_string_columns(df: DataFrame) -> DataFrame:

    skip_keywords = [
        "state",
        "nation",
        "quarter",
        "code",
        "id",
        "zip",
        "phone",
        "affiliated_entity_name",
        "legal_business_name",
        "spl_focus_status"]

    for field in df.schema.fields:
        if (isinstance(field.dataType, StringType)
                and not any(
                keyword in field.name.lower()
                for keyword in skip_keywords)
            ):

            df = df.withColumn(
                field.name,
                F.initcap(F.col(field.name)
                          )
            )

    return df


def apply_boolean_rules(df: DataFrame, boolean_rules: dict) -> DataFrame:

    for column_name, rule in boolean_rules.items():
        if column_name not in df.columns:
            raise ValueError(
                f"Boolean-rule column '{column_name}' does not exist"
            )

        true_values = [
            str(value).strip().upper()
            for value in rule.get("true_values", [])
        ]

        false_values = [
            str(value).strip().upper()
            for value in rule.get("false_values", [])
        ]

        if not true_values or not false_values:
            raise ValueError(
                f"Boolean rule for '{column_name}' must contain "
                "true_values and false_values"
            )

        normalized_value = F.upper(
            F.trim(
                F.col(column_name).cast("string")
            )
        )

        df = df.withColumn(
            column_name,
            F.when(
                normalized_value.isin(
                    true_values
                ),
                F.lit(True)
            )
            .when(
                normalized_value.isin(
                    false_values
                ),
                F.lit(False)
            )
            .otherwise(
                F.lit(None).cast("boolean")
            )
        )

    return df


def get_spark_type(type_name: str):

    type_name = type_name.lower().strip()

    if type_name == "string":
        return StringType()

    if type_name in ("integer", "int"):
        return IntegerType()

    if type_name == "double":
        return DoubleType()

    if type_name == "boolean":
        return BooleanType()

    if type_name == "date":
        return DateType()

    if type_name.startswith("decimal("):
        values = (
            type_name
            .replace("decimal(", "")
            .replace(")", "")
            .split(",")
        )

        precision = int(values[0])
        scale = int(values[1])

        return DecimalType(precision, scale)

    raise ValueError(
        f"Unsupported data type: {type_name}"
    )


def cast_columns(df: DataFrame, data_types: dict, date_columns: list) -> DataFrame:

    date_columns = set(date_columns)

    for column_name, type_name in data_types.items():

        if column_name not in df.columns:
            raise ValueError(
                f"Column missing for type casting: {column_name}"
            )

        # Date columns were already parsed using to_date.
        if column_name in date_columns:
            continue

        df = df.withColumn(
            column_name,
            F.col(column_name).cast(
                get_spark_type(type_name)
            )
        )

    return df


def apply_null_rules(df: DataFrame, null_rules: dict) -> DataFrame:

    fill_values = null_rules.get("fill_values", {})

    missing_columns = [
        column_name for column_name in fill_values if column_name not in df.columns]

    if missing_columns:
        raise ValueError(
            "Null-rule columns do not exist: "
            f"{missing_columns}"
        )

    if fill_values:
        df = df.fillna(fill_values)

    # No transformation is needed for keep_null columns.
    return df


def validate_required_columns(df: DataFrame, required_columns: list):

    missing_columns = [
        column_name for column_name in required_columns if column_name not in df.columns]

    if missing_columns:
        raise ValueError(
            "Configured required columns do not exist: "
            f"{missing_columns}"
        )

    result = df.withColumn(
        "_validation_error",
        F.lit(None).cast("string")
    )

    for column_name in required_columns:

        invalid_condition = (
            F.col(column_name).isNull()
            | (
                F.trim(
                    F.col(column_name).cast("string")
                ) == ""
            )
        )

        error_message = F.lit(f"{column_name} is required")

        result = result.withColumn(
            "_validation_error",
            F.when(
                invalid_condition,
                F.when(
                    F.col("_validation_error").isNull(),
                    error_message
                ).otherwise(
                    F.concat_ws(
                        "; ",
                        F.col("_validation_error"),
                        error_message
                    )
                )
            ).otherwise(
                F.col("_validation_error")
            )
        )

    valid_df = (result.filter(
        F.col("_validation_error").isNull()).drop("_validation_error"))
    invalid_df = result.filter(F.col("_validation_error").isNotNull())

    return valid_df, invalid_df


def convert_date_columns(df: DataFrame, date_columns: list, date_formats: dict) -> DataFrame:

    schema_types = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    for column_name in date_columns:

        if column_name not in df.columns:
            raise ValueError(
                f"Date column '{column_name}' does not exist"
            )

        # Do not reparse a column already stored as DateType.
        if isinstance(schema_types[column_name], DateType):
            print(
                f"{column_name} is already DateType; "
                "skipping conversion"
            )
            continue

        configured_formats = date_formats.get(column_name)

        if not configured_formats:
            raise ValueError(
                f"No date format configured for '{column_name}'"
            )

        if isinstance(configured_formats, str):
            configured_formats = [configured_formats]

        source_value = F.trim(F.col(column_name).cast("string"))

        parsed_values = [F.to_date(source_value, date_format)
                         for date_format in configured_formats]

        df = df.withColumn(
            column_name,
            F.coalesce(*parsed_values)
        )

    return df


def get_duplicate_columns(table_config: dict) -> list:

    duplicate_config = table_config["duplicate_validation"]

    if duplicate_config.get("use_primary_key", True):
        primary_key = table_config["primary_key"]

        if isinstance(primary_key, str):
            return [primary_key]

        return primary_key

    return duplicate_config["columns"]


def validate_duplicates(df: DataFrame, table_config: dict):

    duplicate_config = table_config.get(
        "duplicate_validation",
        {}
    )

    empty_invalid_df = (
        df.limit(0)
        .withColumn(
            "_validation_error",
            F.lit(None).cast("string")
        )
    )

    if not duplicate_config.get("enabled", False):
        return df, empty_invalid_df

    duplicate_columns = get_duplicate_columns(
        table_config
    )

    missing_columns = [
        column_name
        for column_name in duplicate_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Duplicate-validation columns do not exist: "
            f"{missing_columns}"
        )

    duplicate_window = Window.partitionBy(
        *duplicate_columns
    )

    checked_df = df.withColumn(
        "_duplicate_count",
        F.count(F.lit(1)).over(duplicate_window)
    )

    duplicate_df = (
        checked_df
        .filter(F.col("_duplicate_count") > 1)
        .drop("_duplicate_count")
        .withColumn(
            "_validation_error",
            F.lit("Duplicate key in incoming batch")
        )
    )

    valid_df = (
        checked_df
        .filter(F.col("_duplicate_count") == 1)
        .drop("_duplicate_count")
    )

    return valid_df, duplicate_df


def combine_quarantine_dataframes(required_invalid_df: DataFrame, duplicate_invalid_df: DataFrame) -> DataFrame:

    return required_invalid_df.unionByName(
        duplicate_invalid_df,
        allowMissingColumns=True
    )


def remove_leading_zeros(df: DataFrame, columns: list) -> DataFrame:

    for column_name in columns:

        if column_name not in df.columns:
            raise ValueError(
                f"Leading-zero column '{column_name}' does not exist."
            )

        df = df.withColumn(
            column_name,
            F.when(
                F.col(column_name).isNull(),
                None
            ).otherwise(
                F.regexp_replace(
                    F.col(column_name).cast("string"),
                    r"^0+",
                    ""
                )
            )
        )

    return df


def write_full_delta_table(df: DataFrame, table_name: str, table_config: dict, silver_bucket: str) -> None:

    write_mode = table_config.get(
        "write_mode",
        "overwrite"
    ).lower()

    if write_mode != "overwrite":
        raise ValueError(
            f"FULL table '{table_name}' must use "
            "write_mode='overwrite'. Received "
            f"'{write_mode}'."
        )

    if not df.take(1):
        raise ValueError(
            f"Refusing to overwrite FULL table "
            f"'{table_name}' with an empty DataFrame"
        )

    target_path = (
        f"s3://{silver_bucket}/"
        f"{table_config['target_path'].strip('/')}/"
    )

    partition_columns = table_config.get(
        "partition_columns",
        []
    )

    if isinstance(partition_columns, str):
        partition_columns = [partition_columns]

    missing_partition_columns = [
        column_name
        for column_name in partition_columns
        if column_name not in df.columns
    ]

    if missing_partition_columns:
        raise ValueError(
            f"Partition columns do not exist for "
            f"table '{table_name}': "
            f"{missing_partition_columns}"
        )

    print(
        f"Writing FULL Delta snapshot for "
        f"{table_name} to {target_path}"
    )

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_columns:
        print(
            f"{table_name}: Partitioning by "
            f"{partition_columns}"
        )

        writer = writer.partitionBy(
            *partition_columns
        )
    else:
        print(
            f"{table_name}: No partitioning configured"
        )

    writer.save(target_path)

    print(
        f"{table_name}: FULL Delta write completed"
    )


def write_scd2_delta_table(spark, df: DataFrame, table_name: str, table_config: dict, silver_bucket: str) -> None:

    scd2_config = table_config.get("scd2", {})
    business_keys = scd2_config["business_keys"]
    columns_config = scd2_config.get("columns", {})
    hash_column = columns_config.get("record_hash", "scd_record_hash")
    start_column = columns_config.get(
        "effective_start_date", "effective_start_date")
    end_column = columns_config.get("effective_end_date", "effective_end_date")
    current_column = columns_config.get("current_flag", "is_current")
    insert_timestamp_column = columns_config.get(
        "insert_timestamp", "scd_insert_timestamp")
    update_timestamp_column = columns_config.get(
        "update_timestamp", "scd_update_timestamp")
    effective_date_column = scd2_config["effective_date_column"]

    target_path = (
        f"s3://{silver_bucket}/"
        f"{table_config['target_path'].strip('/')}/"
    )

    if not df.take(1):
        raise ValueError(
            f"Refusing to process empty SCD2 input "
            f"for table '{table_name}'"
        )

    source_df = prepare_scd2_source(
        df=df,
        table_name=table_name,
        table_config=table_config
    )

    source_df = source_df.persist()

    try:
        source_count = source_df.count()

        print(
            f"{table_name}: Prepared "
            f"{source_count} SCD2 source rows"
        )

        target_exists = DeltaTable.isDeltaTable(
            spark,
            target_path
        )

        # First SCD2 load
        if not target_exists:

            print(
                f"{table_name}: Target Delta table "
                "does not exist. Creating initial "
                "SCD2 table."
            )

            (
                source_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )

            print(
                f"{table_name}: Initial SCD2 Delta "
                "table created successfully"
            )

            return

        target_delta = DeltaTable.forPath(
            spark,
            target_path
        )

        target_current_df = (
            target_delta
            .toDF()
            .filter(F.col(current_column) == True)
        )

        key_condition = build_key_condition(
            business_keys=business_keys,
            target_alias="target",
            source_alias="source"
        )

        comparison_condition = build_key_condition(
            business_keys=business_keys,
            target_alias="target",
            source_alias="source"
        )

        # Find new providers and changed providers.
        comparison_df = (
            source_df.alias("source")
            .join(
                target_current_df.alias("target"),
                F.expr(comparison_condition),
                "left"
            )
        )

        first_business_key = business_keys[0]

        new_rows_df = (
            comparison_df
            .filter(
                F.col(
                    f"target.{first_business_key}"
                ).isNull()
            )
            .select("source.*")
        )

        changed_rows_df = (
            comparison_df
            .filter(
                F.col(
                    f"target.{first_business_key}"
                ).isNotNull()
                & (
                    ~F.col(
                        f"source.{hash_column}"
                    ).eqNullSafe(
                        F.col(
                            f"target.{hash_column}"
                        )
                    )
                )
            )
            .select("source.*")
        )

        unchanged_rows_df = (
            comparison_df
            .filter(
                F.col(
                    f"target.{first_business_key}"
                ).isNotNull()
                & F.col(
                    f"source.{hash_column}"
                ).eqNullSafe(
                    F.col(
                        f"target.{hash_column}"
                    )
                )
            )
            .select("source.*")
        )

        new_rows_df = new_rows_df.persist()
        changed_rows_df = changed_rows_df.persist()
        unchanged_rows_df = unchanged_rows_df.persist()

        try:
            new_count = new_rows_df.count()
            changed_count = changed_rows_df.count()
            unchanged_count = unchanged_rows_df.count()

            print(
                f"{table_name}: New providers = "
                f"{new_count}"
            )

            print(
                f"{table_name}: Changed providers = "
                f"{changed_count}"
            )

            print(
                f"{table_name}: Unchanged providers = "
                f"{unchanged_count}"
            )

            # Step 1: Close existing current rows that changed.
            if changed_count > 0:

                close_source_df = (
                    changed_rows_df
                    .select(
                        *business_keys,
                        F.col(
                            effective_date_column
                        ).alias(
                            "_new_effective_date"
                        )
                    )
                )

                close_condition = (
                    build_key_condition(
                        business_keys=business_keys,
                        target_alias="target",
                        source_alias="source"
                    )
                    + f" AND target.`{current_column}` = true"
                )

                update_values = {
                    end_column: (
                        "date_sub("
                        "source._new_effective_date, 1"
                        ")"
                    ),
                    current_column: "false",
                    update_timestamp_column: (
                        "current_timestamp()"
                    )
                }

                (
                    target_delta.alias("target")
                    .merge(
                        close_source_df.alias("source"),
                        close_condition
                    )
                    .whenMatchedUpdate(
                        set=update_values
                    )
                    .execute()
                )

                print(
                    f"{table_name}: Closed "
                    f"{changed_count} old versions"
                )

            # Step 2: Insert brand-new providers and
            # new versions of changed providers.
            rows_to_insert_df = (
                new_rows_df
                .unionByName(
                    changed_rows_df,
                    allowMissingColumns=False
                )
            )

            rows_to_insert_count = (
                new_count + changed_count
            )

            if rows_to_insert_count > 0:

                (
                    rows_to_insert_df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(target_path)
                )

                print(
                    f"{table_name}: Inserted "
                    f"{rows_to_insert_count} current "
                    "SCD2 versions"
                )
            else:
                print(
                    f"{table_name}: No new or changed "
                    "rows to insert"
                )

        finally:
            new_rows_df.unpersist()
            changed_rows_df.unpersist()
            unchanged_rows_df.unpersist()

    finally:
        source_df.unpersist()


def write_silver_table(spark, df: DataFrame, table_name: str, table_config: dict, silver_bucket: str) -> None:

    load_type = table_config.get("load_type", "FULL").upper()

    if load_type == "FULL":

        write_full_delta_table(
            df=df,
            table_name=table_name,
            table_config=table_config,
            silver_bucket=silver_bucket
        )

    elif load_type == "SCD2":

        write_mode = table_config.get("write_mode", "merge").lower()

        if write_mode != "merge":
            raise ValueError(
                f"SCD2 table '{table_name}' must use "
                f"write_mode='merge'. Received "
                f"'{write_mode}'."
            )

        write_scd2_delta_table(
            spark=spark,
            df=df,
            table_name=table_name,
            table_config=table_config,
            silver_bucket=silver_bucket
        )

    else:
        raise ValueError(
            f"Unsupported load_type '{load_type}' "
            f"for table '{table_name}'"
        )


def write_quarantine_table(df: DataFrame, table_name: str, quarantine_bucket: str, run_id: str) -> None:
    if not df.head(1):
        print(
            f"No quarantine records for "
            f"{table_name}"
        )
        return

    quarantine_path = (
        f"s3://{quarantine_bucket}/"
        f"delta/test/quarantine/{table_name}/"
    )

    output_df = (
        df
        .withColumn(
            "_quarantine_run_id",
            F.lit(run_id)
        )
        .withColumn(
            "_quarantine_timestamp",
            F.current_timestamp()
        )
    )

    print(
        f"Writing quarantine records to "
        f"{quarantine_path}"
    )

    (
        output_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(quarantine_path)
    )


def transform_using_config(spark, table_name: str, table_config: dict, bronze_bucket: str):

    df = read_bronze_table(spark, bronze_bucket, table_config)

    df = select_source_snapshot(
        df=df,
        table_name=table_name,
        table_config=table_config
    )

    print(
        f"{table_name}: Rows after snapshot "
        f"filtering = {df.count()}")

    df = columns_renaming(df, table_config.get("rename_columns", {}))

    df = clean_string_columns(df)

    df = title_case_string_columns(df)

    df = remove_leading_zeros(df, table_config.get("remove_leading_zeros", []))

    df = apply_boolean_rules(df, table_config.get("boolean_rules", {}))

    date_columns = table_config.get("date_columns", [])

    df = convert_date_columns(
        df, date_columns, table_config.get("date_formats", {}))

    df = cast_columns(df, table_config.get("data_types", {}), date_columns)

    df = round_numeric_columns(df)

    df = apply_null_rules(df, table_config.get("null_rules", {}))

    df, required_invalid_df = validate_required_columns(
        df, table_config.get("required_columns", []))

    df, duplicate_invalid_df = validate_duplicates(df, table_config)

    quarantine_df = combine_quarantine_dataframes(
        required_invalid_df, duplicate_invalid_df)

    return df, quarantine_df


def main() -> None:

    config = load_config_json(
        "s3://healthcare-proj-data-bkt/config/"
        "silver_tables_config.json"
    )

    run_id = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    table_name = args["TABLE_NAME"]

    # -----------------------------------------------------
    # Validate configured table
    # -----------------------------------------------------

    if table_name not in config["tables"]:

        raise ValueError(
            f"Table '{table_name}' is not configured "
            f"in silver_tables_config.json"
        )

    table_config = config["tables"][
        table_name
    ]

    print(
        f"Starting Silver processing for: "
        f"{table_name}"
    )

    # -----------------------------------------------------
    # Transform Bronze -> Silver
    # -----------------------------------------------------

    valid_df, quarantine_df = (
        transform_using_config(
            spark=spark,
            table_name=table_name,
            table_config=table_config,
            bronze_bucket=BRONZE_BUCKET
        )
    )

    # -----------------------------------------------------
    # Write invalid records
    # -----------------------------------------------------

    write_quarantine_table(
        df=quarantine_df,
        table_name=table_name,
        quarantine_bucket=QUARANTINE_BUCKET,
        run_id=run_id
    )

    # -----------------------------------------------------
    # Write Silver table
    # -----------------------------------------------------

    write_silver_table(
        spark=spark,
        df=valid_df,
        table_name=table_name,
        table_config=table_config,
        silver_bucket=SILVER_BUCKET
    )

    print(
        f"{table_name}: Silver processing "
        f"completed successfully"
    )


if __name__ == "__main__":

    try:

        main()

        job.commit()

        print(
            "Silver Glue job "
            "completed successfully."
        )

    except Exception as error:

        print(
            f"Silver Glue job failed: "
            f"{error}"
        )

        raise
