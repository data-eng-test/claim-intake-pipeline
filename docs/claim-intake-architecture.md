# claim-intake Architecture

## Data Flow
Provider/Hospital APIs
    → Python ingest scripts (src/ingest/)
    → S3 raw zone (s3://insurer-claims-raw/{provider|hospital}/{date}/)
    → AWS Glue normalise job
    → S3 processed zone (s3://insurer-claims-processed/normalised/)
    → Redshift COPY → claims_raw.stg_claim_submissions
    → dbt stg_claim_raw (dedup + standardise)
    → claims_staging.stg_claim_raw
    → consumed by claim-adjudication-engine

## SLA
- Claims available in staging within 2 hours of API submission
- DAG runs every 4 hours — worst case 2hr processing + 4hr schedule = 6hr lag
- P1 breach: page claims-oncall@insurer.com immediately

## Known Issues
- CLAIM-001: Hospital API v1 still used by 3 networks — v2 migration pending
- CLAIM-004: Duplicate submissions from clearinghouse RELAY-X on Tuesdays
- No schema validation on raw JSON landing — malformed claims silently skipped

## Dependencies
- Upstream: 12 hospital networks, 4 clearinghouses, provider credentialing DB
- Downstream: claim-adjudication-engine reads claims_staging.stg_claim_raw
