"""
redshift_loader.py
Loads normalised Parquet from S3 processed zone into Redshift using COPY command.
Target: claims_raw.stg_claim_submissions
Called by Airflow S3ToRedshiftOperator — this module provides the SQL helpers.
"""
import boto3
import psycopg2
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

REDSHIFT_CONFIG = {
    "host":     "claims-prod-cluster.us-east-1.redshift.amazonaws.com",
    "port":     5439,
    "dbname":   "claimsdb",
    "user":     "claims_loader",
}
IAM_ROLE = "arn:aws:iam::123456789:role/claims-redshift-copy-role"


def get_redshift_connection():
    secret = _get_secret("claims/redshift/loader")
    return psycopg2.connect(
        host=REDSHIFT_CONFIG["host"],
        port=REDSHIFT_CONFIG["port"],
        dbname=REDSHIFT_CONFIG["dbname"],
        user=REDSHIFT_CONFIG["user"],
        password=secret["password"],
        sslmode="require",
    )


def load_to_staging(s3_path: str, source_date: str) -> int:
    """
    COPY normalised Parquet from S3 into Redshift staging table.
    Uses INSERT INTO ... SELECT to support incremental loads.
    Returns number of rows loaded.
    """
    conn = get_redshift_connection()
    cur = conn.cursor()

    # Create temp table for this load batch
    temp_table = f"claims_raw.stg_claim_submissions_tmp_{source_date.replace('-', '')}"
    cur.execute(f"""
        CREATE TEMP TABLE {temp_table} (LIKE claims_raw.stg_claim_submissions);
    """)

    # COPY from S3
    cur.execute(f"""
        COPY {temp_table}
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        SERIALIZETOJSON;
    """)

    # Upsert into main staging table (delete + insert)
    cur.execute(f"""
        DELETE FROM claims_raw.stg_claim_submissions
        WHERE claim_submission_id IN (SELECT claim_submission_id FROM {temp_table});
    """)
    cur.execute(f"""
        INSERT INTO claims_raw.stg_claim_submissions
        SELECT * FROM {temp_table};
    """)

    # Get row count
    cur.execute(f"SELECT COUNT(*) FROM {temp_table};")
    row_count = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Loaded {row_count} rows into claims_raw.stg_claim_submissions from {s3_path}")
    return row_count


def _get_secret(secret_name: str) -> dict:
    import json
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
