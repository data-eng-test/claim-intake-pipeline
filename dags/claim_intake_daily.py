"""
claim_intake_daily.py
Airflow DAG — ingests claim submissions from all registered providers every 4 hours.
Lands raw JSON in S3, triggers Glue normalisation job, loads to Redshift staging.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "claims-data-eng",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": True,
    "email":            ["claims-oncall@insurer.com"],
}

with DAG(
    dag_id="claim_intake_daily",
    default_args=default_args,
    schedule_interval="0 */4 * * *",   # every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["claims", "intake", "ingestion"],
) as dag:

    fetch_provider_claims = PythonOperator(
        task_id="fetch_provider_claims",
        python_callable=fetch_all_providers,
        op_kwargs={"s3_bucket": "insurer-claims-raw", "prefix": "provider/"},
    )

    fetch_hospital_claims = PythonOperator(
        task_id="fetch_hospital_claims",
        python_callable=fetch_all_hospitals,
        op_kwargs={"s3_bucket": "insurer-claims-raw", "prefix": "hospital/"},
    )

    normalise_claims = GlueJobOperator(
        task_id="normalise_claims",
        job_name="claim-normalise-glue-job",
        script_location="s3://insurer-glue-scripts/claim_normalise.py",
        aws_conn_id="aws_claims_prod",
    )

    load_to_redshift = S3ToRedshiftOperator(
        task_id="load_to_redshift",
        schema="claims_raw",
        table="stg_claim_submissions",
        s3_bucket="insurer-claims-processed",
        s3_key="normalised/",
        copy_options=["FORMAT AS PARQUET"],
        aws_conn_id="aws_claims_prod",
        redshift_conn_id="redshift_claims_prod",
    )

    [fetch_provider_claims, fetch_hospital_claims] >> normalise_claims >> load_to_redshift
