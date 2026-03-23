"""
provider_api.py
Fetches claim submissions from third-party provider REST APIs.
Supports 4 clearinghouses: RELAY-X, AVAILITY, WAYSTAR, CHANGE_HEALTH
Lands raw JSON to S3 raw zone: s3://insurer-claims-raw/provider/{date}/
"""
import boto3
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

PROVIDER_CONFIGS = {
    "RELAY-X": {
        "base_url": "https://api.relay-x.com/v2/claims",
        "auth_type": "oauth2",
        "token_url": "https://api.relay-x.com/oauth/token",
        "client_id": "INSURER_PROD_001",
        "scopes":    ["claims.read", "eligibility.read"],
    },
    "AVAILITY": {
        "base_url": "https://api.availity.com/availity/v1/claims",
        "auth_type": "oauth2",
        "token_url": "https://api.availity.com/oauth2/token",
        "client_id": "INSURER_AVAILITY_002",
        "scopes":    ["claims:read"],
    },
    "WAYSTAR": {
        "base_url": "https://api.waystar.com/claims/v3",
        "auth_type": "api_key",
        "header":    "X-Waystar-Key",
    },
    "CHANGE_HEALTH": {
        "base_url": "https://api.changehealthcare.com/medicalclaims/v3",
        "auth_type": "oauth2",
        "token_url": "https://api.changehealthcare.com/oauth/token",
        "client_id": "INSURER_CH_003",
        "scopes":    ["claims.read"],
    },
}

S3_BUCKET = "insurer-claims-raw"
s3_client = boto3.client("s3", region_name="us-east-1")


def get_oauth_token(config: dict) -> str:
    """Fetch OAuth2 access token for a provider."""
    import os
    secret = _get_secret(f"claims/provider/{config['client_id']}")
    resp = requests.post(
        config["token_url"],
        data={
            "grant_type":    "client_credentials",
            "client_id":     config["client_id"],
            "client_secret": secret["client_secret"],
            "scope":         " ".join(config.get("scopes", [])),
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_claims_from_provider(
    provider_name: str,
    since: Optional[datetime] = None,
    env: str = "prod",
) -> int:
    """
    Fetch all claim submissions from a provider since the given datetime.
    Lands raw JSON to S3. Returns count of claims fetched.
    """
    config = PROVIDER_CONFIGS[provider_name]
    since = since or (datetime.utcnow() - timedelta(hours=4))
    date_str = datetime.utcnow().strftime("%Y/%m/%d")
    s3_prefix = f"provider/{provider_name}/{date_str}/"

    # Authenticate
    if config["auth_type"] == "oauth2":
        token = get_oauth_token(config)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    else:
        api_key = _get_secret(f"claims/provider/{provider_name}")["api_key"]
        headers = {config["header"]: api_key, "Content-Type": "application/json"}

    # Paginated fetch
    page, total_records = 1, 0
    while True:
        params = {
            "submittedAfter": since.isoformat() + "Z",
            "pageSize": 500,
            "page": page,
            "status": "SUBMITTED",
        }
        resp = requests.get(config["base_url"], headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        claims = payload.get("claims", payload.get("data", []))

        if not claims:
            break

        # Land to S3
        s3_key = f"{s3_prefix}{provider_name}_claims_page{page}_{datetime.utcnow().strftime('%H%M%S')}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps({
                "provider":    provider_name,
                "fetched_at":  datetime.utcnow().isoformat(),
                "page":        page,
                "record_count": len(claims),
                "claims":      claims,
            }),
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )

        total_records += len(claims)
        logger.info(f"{provider_name} page {page}: {len(claims)} claims → s3://{S3_BUCKET}/{s3_key}")

        if len(claims) < 500 or not payload.get("hasMore", True):
            break
        page += 1

    logger.info(f"{provider_name}: total {total_records} claims fetched")
    return total_records


def fetch_all_providers(s3_bucket: str, prefix: str) -> dict:
    """Fetch claims from all configured providers. Called by Airflow task."""
    results = {}
    for provider_name in PROVIDER_CONFIGS:
        try:
            count = fetch_claims_from_provider(provider_name)
            results[provider_name] = {"status": "success", "count": count}
        except Exception as e:
            logger.error(f"{provider_name} fetch failed: {e}")
            results[provider_name] = {"status": "failed", "error": str(e)}
    return results


def _get_secret(secret_name: str) -> dict:
    """Retrieve secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])
