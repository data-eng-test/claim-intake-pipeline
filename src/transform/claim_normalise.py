"""
claim_normalise.py
AWS Glue PySpark job — normalises raw claim JSON from S3 into standard schema.
Reads from: s3://insurer-claims-raw/
Writes to:  s3://insurer-claims-processed/normalised/ (Parquet)
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_date", "s3_raw_bucket", "s3_processed_bucket"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET       = args["s3_raw_bucket"]
PROCESSED_BUCKET = args["s3_processed_bucket"]
SOURCE_DATE      = args["source_date"]

CLAIM_SCHEMA = StructType([
    StructField("claim_submission_id",     StringType(),  False),
    StructField("claim_type",              StringType(),  True),
    StructField("provider_npi",            StringType(),  True),
    StructField("member_id",               StringType(),  True),
    StructField("policy_number",           StringType(),  True),
    StructField("date_of_service",         DateType(),    True),
    StructField("date_submitted",          TimestampType(),True),
    StructField("total_billed_amount_usd", DecimalType(12,2), True),
    StructField("diagnosis_codes",         StringType(),  True),
    StructField("procedure_codes",         StringType(),  True),
    StructField("place_of_service_code",   StringType(),  True),
    StructField("network_status",          StringType(),  True),
    StructField("source_system",           StringType(),  True),
    StructField("_ingested_at",            TimestampType(),True),
])

VALID_CLAIM_TYPES    = ["MEDICAL", "DENTAL", "VISION", "PHARMACY"]
VALID_NETWORK_STATUS = ["IN_NETWORK", "OUT_OF_NETWORK", "UNKNOWN"]


def read_raw_claims():
    """Read all raw JSON claim files for source date."""
    path = f"s3://{RAW_BUCKET}/*/{SOURCE_DATE}/*.json"
    df = spark.read.option("multiline", True).json(path)
    # Explode nested claims array
    df = df.select(
        F.col("provider").alias("source_system"),
        F.col("hospital").alias("hospital_source"),
        F.col("fetched_at"),
        F.explode("claims").alias("claim"),
    )
    return df


def normalise_claims(df):
    """Apply schema normalisation and validation rules."""
    normalised = df.select(
        F.coalesce(
            F.col("claim.claim_submission_id"),
            F.col("claim.ClaimID"),
        ).alias("claim_submission_id"),
        F.upper(F.coalesce(
            F.col("claim.claim_type"),
            F.col("claim.ClaimType"),
        )).alias("claim_type"),
        F.col("claim.provider_npi").alias("provider_npi"),
        F.col("claim.member_id").alias("member_id"),
        F.col("claim.policy_number").alias("policy_number"),
        F.to_date(F.coalesce(
            F.col("claim.date_of_service"),
            F.col("claim.DateOfService"),
        )).alias("date_of_service"),
        F.current_timestamp().alias("date_submitted"),
        F.cast(F.coalesce(
            F.col("claim.total_billed_amount_usd"),
            F.col("claim.BilledAmount"),
        ), DecimalType(12, 2)).alias("total_billed_amount_usd"),
        F.col("claim.diagnosis_codes").alias("diagnosis_codes"),
        F.col("claim.procedure_codes").alias("procedure_codes"),
        F.col("claim.place_of_service_code").alias("place_of_service_code"),
        F.upper(F.coalesce(
            F.col("claim.network_status"),
            F.lit("UNKNOWN"),
        )).alias("network_status"),
        F.coalesce(
            F.col("source_system"),
            F.col("hospital_source"),
        ).alias("source_system"),
        F.current_timestamp().alias("_ingested_at"),
    )

    # Validate claim_type — unknown types marked as UNKNOWN
    normalised = normalised.withColumn(
        "claim_type",
        F.when(F.col("claim_type").isin(VALID_CLAIM_TYPES), F.col("claim_type"))
         .otherwise(F.lit("UNKNOWN"))
    )

    # Drop records with no claim_submission_id (malformed)
    # TODO: Route to quarantine bucket instead (CIP-3)
    normalised = normalised.filter(F.col("claim_submission_id").isNotNull())

    return normalised


def write_parquet(df):
    """Write normalised claims to S3 processed zone as Parquet."""
    output_path = f"s3://{PROCESSED_BUCKET}/normalised/{SOURCE_DATE}/"
    df.write \
      .mode("overwrite") \
      .partitionBy("claim_type") \
      .parquet(output_path)
    count = df.count()
    print(f"Wrote {count} normalised claims to {output_path}")
    return count


raw_df        = read_raw_claims()
normalised_df = normalise_claims(raw_df)
write_parquet(normalised_df)
job.commit()
