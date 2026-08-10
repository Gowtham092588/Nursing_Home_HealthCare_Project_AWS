import sys
import boto3
import json

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DateType,
    LongType,
    DoubleType,
    DecimalType
)
from delta.tables import DeltaTable
from datetime import datetime, timezone


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

STAFFING_SILVER_PATH = (
    "s3://healthcare-proj-data-bkt/"
    "delta/silver/Staffing/"
)

PROVIDER_SILVER_PATH = (
    "s3://healthcare-proj-data-bkt/"
    "delta/silver/NH_Provider/"
)

VACCINATION_SILVER_PATH = (
    "s3://healthcare-proj-data-bkt/"
    "delta/silver/Vaccination/")

USSTATEAVERAGE_SILVER_PATH = (
    "s3://healthcare-proj-data-bkt/"
    "delta/silver/NH_StateUSAverage/")

GOLD_BASE_PATH = (
    "s3://healthcare-proj-data-bkt/delta/gold/"
)

staffing_df = (
    spark.read
    .format("delta")
    .load(STAFFING_SILVER_PATH)
)

provider_df = (
    spark.read
    .format("delta")
    .load(PROVIDER_SILVER_PATH)
    .filter(F.col("is_current") == F.lit(True))
)

vaccination_df = (
    spark.read
    .format("delta")
    .load(VACCINATION_SILVER_PATH)
)

us_state_avg_df = (
    spark.read
    .format("delta")
    .load(USSTATEAVERAGE_SILVER_PATH)
)

# Calculate total care hours per certified bed.

staffing_summary_df = (
    staffing_df
    .groupby(['provider_id', 'work_date'])
    .agg(F.sum("hrs_rn").alias("total_rn_hours"),
         F.sum("hrs_lpn").alias("total_lpn_hours"),
         F.sum("hrs_cna").alias("total_cna_hours")
         )
)

staffing_summary_df = (
    staffing_summary_df
    .withColumn(
        "total_care_hours",
        F.round(
            F.coalesce(F.col("total_rn_hours"), F.lit(0.0))
            + F.coalesce(F.col("total_lpn_hours"), F.lit(0.0))
            + F.coalesce(F.col("total_cna_hours"), F.lit(0.0)), 2
        )
    )
)

provider_capacity_df = (
    provider_df
    .select(
        "provider_id",
        "provider_name",
        "certified_beds"
    )
)

staffing_provider_df = (
    staffing_summary_df.alias('s')
    .join(provider_capacity_df.alias('p'),
          on='provider_id',
          how='left')
)

care_hours_per_bed_df = (
    staffing_provider_df
    .withColumn(
        "total_care_hours_per_certified_bed",
        F.when(
            F.col("certified_beds") > 0,
            F.round(
                F.col("total_care_hours")
                / F.col("certified_beds"),
                2
            )
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
    .select(
        "provider_id",
        "provider_name",
        "work_date",
        "total_rn_hours",
        "total_lpn_hours",
        "total_cna_hours",
        "total_care_hours",
        "certified_beds",
        "total_care_hours_per_certified_bed",
        "load_timestamp"
    )
)

# Bed utilization rate.

provider_bed_utilization_df = (
    provider_df
    .select(
        "provider_id",
        "provider_name",
        "state",
        "certified_beds",
        "avg_residents_per_day"
    )
    .withColumn(
        "bed_utilization_rate",
        F.when(
            F.col("certified_beds") > 0,
            F.round(
                (
                    F.col("avg_residents_per_day")
                    / F.col("certified_beds")
                ) * 100,
                2
            )
        ).otherwise(
            F.lit(None).cast("double")
        )
    )
    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
)

# Average nurse hours per patient  by hospital

nursing_hours_patient_hospital_ratio_df = (
    staffing_df
    .groupBy(
        "provider_id",
        "provider_name",
        "state",
        "work_date"
    )
    .agg(
        F.sum("hrs_rn").alias("total_rn_hours"),
        F.sum("hrs_lpn").alias("total_lpn_hours"),
        F.sum("hrs_cna").alias("total_cna_hours"),
        F.sum("no_of_patients").alias("total_patients")
    )
    .withColumn(
        "total_nursing_hours",
        F.round(
            F.coalesce(
                F.col("total_rn_hours"),
                F.lit(0.0)
            )
            + F.coalesce(
                F.col("total_lpn_hours"),
                F.lit(0.0)
            )
            + F.coalesce(
                F.col("total_cna_hours"),
                F.lit(0.0)
            ), 2
        )
    )

    .withColumn(
        "nurse_to_patient_ratio",
        F.when(
            F.col("total_patients") > 0,
            F.round(
                F.col("total_nursing_hours")
                / F.col("total_patients"),
                2
            )
        ).otherwise(
            F.lit(None).cast("double")
        )
    )

)

# Ratio of permanent staff to temporary/contract staff.

permanent_contract_ratio_df = (
    staffing_df
    .groupBy(
        "provider_id",
        "provider_name",
        "state",
        "work_date"
    )
    .agg(
        F.sum(
            F.coalesce(
                F.col("hrs_rn_emp"),
                F.lit(0.0)
            )
        ).alias("rn_permanent_hours"),

        F.sum(
            F.coalesce(
                F.col("hrs_lpn_emp"),
                F.lit(0.0)
            )
        ).alias("lpn_permanent_hours"),

        F.sum(
            F.coalesce(
                F.col("hrs_cna_emp"),
                F.lit(0.0)
            )
        ).alias("cna_permanent_hours"),

        F.sum(
            F.coalesce(
                F.col("hrs_rn_ctr"),
                F.lit(0.0)
            )
        ).alias("rn_contract_hours"),

        F.sum(
            F.coalesce(
                F.col("hrs_lpn_ctr"),
                F.lit(0.0)
            )
        ).alias("lpn_contract_hours"),

        F.sum(
            F.coalesce(
                F.col("hrs_cna_ctr"),
                F.lit(0.0)
            )
        ).alias("cna_contract_hours")
    )

    .withColumn(
        "total_permanent_hours",
        F.round(
            F.col("rn_permanent_hours")
            + F.col("lpn_permanent_hours")
            + F.col("cna_permanent_hours"),
            2
        )
    )

    .withColumn(
        "total_contract_hours",
        F.round(
            F.col("rn_contract_hours")
            + F.col("lpn_contract_hours")
            + F.col("cna_contract_hours"),
            2
        )
    )

    .withColumn(
        "permanent_to_contract_ratio",
        F.when(
            F.col("total_contract_hours") > 0,
            F.round(
                F.col("total_permanent_hours")
                / F.col("total_contract_hours"),
                2
            )
        ).otherwise(
            F.lit(None).cast("double")
        )
    )

    .withColumn(
        "permanent_staff_percentage",
        F.when(
            (
                F.col("total_permanent_hours")
                + F.col("total_contract_hours")
            ) > 0,
            F.round(
                (
                    F.col("total_permanent_hours")
                    / (
                        F.col("total_permanent_hours")
                        + F.col("total_contract_hours")
                    )
                ) * 100,
                2
            )
        ).otherwise(
            F.lit(None).cast("double")
        )
    )

    .withColumn(
        "contract_staff_percentage",
        F.when(
            (
                F.col("total_permanent_hours")
                + F.col("total_contract_hours")
            ) > 0,
            F.round(
                (
                    F.col("total_contract_hours")
                    / (
                        F.col("total_permanent_hours")
                        + F.col("total_contract_hours")
                    )
                ) * 100,
                2
            )
        ).otherwise(
            F.lit(None).cast("double")
        )
    )
)

provider_type_df = (
    provider_df
    .select(
        "provider_id",
        "provider_type"
    )
)

permanent_contract_ratio_df = (
    permanent_contract_ratio_df.alias("s")
    .join(
        provider_type_df.alias("p"),
        on="provider_id",
        how="left"
    )
    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
    .select(
        "provider_id",
        "provider_name",
        "state",
        "provider_type",
        "work_date",
        "rn_permanent_hours",
        "lpn_permanent_hours",
        "cna_permanent_hours",
        "rn_contract_hours",
        "lpn_contract_hours",
        "cna_contract_hours",
        "total_permanent_hours",
        "total_contract_hours",
        "permanent_to_contract_ratio",
        "permanent_staff_percentage",
        "contract_staff_percentage",
        "load_timestamp"
    )
)

# Average Readmission rates within 30 days by state

state_healthcare_metrics_df = (
    us_state_avg_df
    .groupBy("state_nation")
    .agg(
        F.round(
            F.avg(
                "percentage_short_stay_residents_rehospitalized_after_nursing_home_admission"
            ),
            2
        ).alias("avg_short_stay_rehospitalization_rate_pct"),

        F.round(
            F.avg(
                "percentage_short_stay_residents_outpatient_emergency_department_visit"
            ),
            2
        ).alias("avg_short_stay_op_emergency_dept_visit"),

        F.round(
            F.avg(
                "number_hospitalizations_1000_long_stay_resident_days"
            ),
            2
        ).alias("avg_hospitalizations_per_1000_resident_days"),

        F.round(
            F.avg(
                "number_outpatient_emergency_department_visits_1000_long_stay_resident_days"
            ),
            2
        ).alias("avg_ed_visits_per_1000_resident_days")
    )
    .orderBy("state_nation")
)
# Resident vs Staff Vaccination Comparison

vaccination_comparison_df = (
    vaccination_df
    .groupBy("state")
    .agg(
        F.round(
            F.avg("resident_vaccine_percentage"),
            2
        ).alias("resident_rate"),
        F.round(
            F.avg("staff_vaccine_percentage"),
            2
        ).alias("staff_rate")
    )
)

# Validate


def validate_not_empty(df, table_name):
    if not df.take(1):
        raise ValueError(
            f"Refusing to write empty Gold table: {table_name}"
        )


def validate_unique(df, key_columns, table_name):
    duplicate_exists = (
        df
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_exists > 0:
        raise ValueError(
            f"{table_name} contains duplicate records "
            f"for key {key_columns}"
        )


def write_gold_table(dataframe, table_name, mode="overwrite", partition_columns=None):

    if dataframe is None:
        raise ValueError(
            f"DataFrame for {table_name} is None"
        )

    if not dataframe.take(1):
        raise ValueError(
            f"Refusing to write empty Gold table: {table_name}"
        )

    target_path = (
        f"{GOLD_BASE_PATH}"
        f"{table_name.strip('/')}/"
    )

    writer = (
        dataframe
        .write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
    )

    if partition_columns:
        missing_columns = [
            column
            for column in partition_columns
            if column not in dataframe.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Cannot partition {table_name}. "
                f"Missing columns: {missing_columns}"
            )

        writer = writer.partitionBy(
            *partition_columns
        )

    writer.save(target_path)

    if DeltaTable.isDeltaTable(spark, target_path):
        print(f"{table_name}: Delta table successfully created")
    else:
        raise ValueError(f"{table_name}: Target path is not a Delta table")


def main() -> None:

    try:
        print("Starting Gold-layer validation")

        validate_unique(
            care_hours_per_bed_df,
            ["provider_id", "work_date"],
            "total_care_hours_per_certified_bed"
        )

        validate_unique(
            provider_bed_utilization_df,
            ["provider_id"],
            "provider_bed_utilization"
        )

        validate_unique(
            nursing_hours_patient_hospital_ratio_df,
            ["provider_id", "work_date"],
            "nursing_hours_per_patient_hospital"
        )

        validate_unique(
            permanent_contract_ratio_df,
            ["provider_id", "work_date"],
            "permanent_contract_staffing_ratio"
        )

        validate_unique(
            state_healthcare_metrics_df,
            ["state_nation"],
            "state_healthcare_metrics"
        )

        validate_unique(
            vaccination_comparison_df,
            ["state"],
            "resident_staff_vaccination_comparison"
        )

        print("Gold-layer validation completed successfully")

        print("Starting Gold-layer writes")

        write_gold_table(
            dataframe=care_hours_per_bed_df,
            table_name="total_care_hours_per_certified_bed",
            partition_columns=["work_date"]
        )

        write_gold_table(
            dataframe=provider_bed_utilization_df,
            table_name="provider_bed_utilization"
        )

        write_gold_table(
            dataframe=nursing_hours_patient_hospital_ratio_df,
            table_name="avg_nurse_hours_to_patient_hospital"
        )

        write_gold_table(
            dataframe=permanent_contract_ratio_df,
            table_name="permanent_contract_staffing_ratio",
            partition_columns=["work_date"]
        )

        write_gold_table(
            dataframe=state_healthcare_metrics_df,
            table_name="state_healthcare_metrics"
        )
        write_gold_table(
            dataframe=vaccination_comparison_df,
            table_name="resident_staff_vaccination_comparison"
        )

        print("All Gold Delta tables written successfully")

        job.commit()

    except Exception as error:
        print(
            "Gold Glue job failed: "
            f"{error}"
        )
        raise


if __name__ == "__main__":
    main()
