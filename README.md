# claim-intake-pipeline

Ingests raw insurance claims from hospital and third-party provider REST APIs.
Claims are landed in S3 raw zone, normalised, and loaded to Redshift staging.

## Overview
- Source: 12 hospital networks + 4 third-party clearinghouses
- Destination: Redshift `claims_raw.stg_claim_submissions`
- Schedule: Every 4 hours via Airflow (Cloud Composer equivalent: MWAA on AWS)
- SLA: Claims must be in Redshift staging within 2 hours of submission

## Tech Stack
- Orchestration: Apache Airflow 2.8 on Amazon MWAA
- Ingestion: Python 3.11 (boto3, requests)
- Storage: S3 (raw zone: s3://insurer-claims-raw/)
- Warehouse: Amazon Redshift (cluster: claims-prod-cluster)
- Transformation: AWS Glue PySpark jobs
- IaC: Terraform

## Running Locally
```bash
pip install -r requirements.txt
export AWS_PROFILE=claims-dev
python src/ingest/provider_api.py --env dev --keyword claim
```

## Key Contacts
- Pipeline Owner: Priya Sharma (priya.sharma@insurer.com)
- On-call: #claims-data-alerts (Slack)
- SLA breach escalation: claims-oncall@insurer.com
