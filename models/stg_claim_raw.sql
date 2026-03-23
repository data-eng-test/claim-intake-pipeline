-- stg_claim_raw.sql
-- Staging model: deduplicates and standardises raw claim submissions
-- Source: claims_raw.stg_claim_submissions (loaded by claim_intake_daily DAG)
-- Output: claims_staging.stg_claim_raw

{{ config(
    materialized = 'incremental',
    unique_key   = 'claim_submission_id',
    schema       = 'claims_staging'
) }}

SELECT
    claim_submission_id,
    claim_type,                          -- MEDICAL, DENTAL, VISION, PHARMACY
    provider_npi,                        -- National Provider Identifier
    member_id,
    policy_number,
    date_of_service,
    date_submitted,
    total_billed_amount_usd,
    diagnosis_codes,                     -- ICD-10 array
    procedure_codes,                     -- CPT code array
    place_of_service_code,
    network_status,                      -- IN_NETWORK, OUT_OF_NETWORK
    source_system,                       -- PROVIDER_API, HOSPITAL_API, CLEARINGHOUSE
    _ingested_at,
    ROW_NUMBER() OVER (
        PARTITION BY claim_submission_id
        ORDER BY _ingested_at DESC
    ) AS rn
FROM {{ source('claims_raw', 'stg_claim_submissions') }}
WHERE rn = 1

{% if is_incremental() %}
    AND _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
