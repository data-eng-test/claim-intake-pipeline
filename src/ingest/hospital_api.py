"""
hospital_api.py
Fetches claim submissions from hospital network APIs.
Supports both v1 (XML, Basic Auth) and v2 (JSON, OAuth2) endpoints.
12 hospital networks configured — 9 on v2, 3 still on v1 (migration pending CIP-2).
"""
import boto3
import requests
import json
import xml.etree.ElementTree as ET
import base64
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

HOSPITAL_CONFIGS = {
    # v2 hospitals — JSON + OAuth2
    "METRO_HEALTH_SYSTEM":    {"version": "v2", "base_url": "https://api.metrohealthsys.com/claims/v2"},
    "SUNRISE_MEDICAL_CENTER": {"version": "v2", "base_url": "https://api.sunrisemedical.com/claims/v2"},
    "COASTAL_GENERAL":        {"version": "v2", "base_url": "https://api.coastalgeneral.org/fhir/claims/v2"},
    "VALLEY_HEALTH_NETWORK":  {"version": "v2", "base_url": "https://api.valleyhealthnet.com/claims/v2"},
    "PINNACLE_HOSPITALS":     {"version": "v2", "base_url": "https://api.pinnaclehospitals.com/api/claims/v2"},
    "RIVERSIDE_HEALTH":       {"version": "v2", "base_url": "https://claims.riversidehealth.com/api/v2"},
    "SUMMIT_MEDICAL_GROUP":   {"version": "v2", "base_url": "https://api.summitmg.com/claims/v2"},
    "CLEARWATER_REGIONAL":    {"version": "v2", "base_url": "https://api.clearwaterregional.com/claims/v2"},
    "HERITAGE_HEALTH":        {"version": "v2", "base_url": "https://api.heritagehealth.com/claims/v2"},
    # v1 hospitals — XML + Basic Auth (migration pending CIP-1)
    "ST_MARY_REGIONAL":       {"version": "v1", "base_url": "https://claims.stmaryregional.org/api/claims"},
    "LAKESIDE_GENERAL":       {"version": "v1", "base_url": "https://api.lakesidegeneral.com/claims"},
    "NORTHERN_HEALTH_SYSTEM": {"version": "v1", "base_url": "https://claims.northernhealth.net/v1/submissions"},
}

S3_BUCKET = "insurer-claims-raw"
s3_client = boto3.client("s3", region_name="us-east-1")


def fetch_hospital_v2(hospital_name: str, config: dict, since: datetime) -> int:
    """Fetch claims from a v2 JSON hospital API."""
    secret = _get_secret(f"claims/hospital/{hospital_name}")
    token_resp = requests.post(
        config["base_url"].replace("/claims/v2", "/oauth/token"),
        data={
            "grant_type":    "client_credentials",
            "client_id":     secret["client_id"],
            "client_secret": secret["client_secret"],
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    page, total = 1, 0
    date_str = datetime.utcnow().strftime("%Y/%m/%d")

    while True:
        resp = requests.get(
            config["base_url"],
            headers=headers,
            params={"submittedAfter": since.isoformat(), "page": page, "size": 500},
            timeout=60,
        )
        resp.raise_for_status()
        claims = resp.json().get("claims", [])
        if not claims:
            break

        s3_key = f"hospital/{hospital_name}/{date_str}/claims_page{page}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=s3_key,
            Body=json.dumps({"hospital": hospital_name, "api_version": "v2",
                             "fetched_at": datetime.utcnow().isoformat(), "claims": claims}),
            ContentType="application/json", ServerSideEncryption="AES256",
        )
        total += len(claims)
        if len(claims) < 500:
            break
        page += 1
    return total


def fetch_hospital_v1(hospital_name: str, config: dict, since: datetime) -> int:
    """
    Fetch claims from a legacy v1 XML hospital API.
    NOTE: v1 APIs scheduled for deprecation Q3 2026 — migration tracked in CIP-1.
    """
    secret = _get_secret(f"claims/hospital/{hospital_name}")
    creds = base64.b64encode(f"{secret['username']}:{secret['password']}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Accept": "application/xml"}
    date_str = datetime.utcnow().strftime("%Y/%m/%d")

    resp = requests.get(
        config["base_url"],
        headers=headers,
        params={"from_date": since.strftime("%Y-%m-%d"), "format": "xml"},
        timeout=120,
    )
    resp.raise_for_status()

    # Parse XML and convert to JSON for consistent downstream processing
    root = ET.fromstring(resp.content)
    claims = []
    for claim_elem in root.findall(".//Claim"):
        claims.append({
            "claim_submission_id": claim_elem.findtext("ClaimID"),
            "claim_type":          claim_elem.findtext("ClaimType"),
            "provider_npi":        claim_elem.findtext("ProviderNPI"),
            "member_id":           claim_elem.findtext("MemberID"),
            "policy_number":       claim_elem.findtext("PolicyNumber"),
            "date_of_service":     claim_elem.findtext("DateOfService"),
            "total_billed_amount_usd": float(claim_elem.findtext("BilledAmount", "0")),
            "diagnosis_codes":     claim_elem.findtext("DiagnosisCodes", ""),
            "procedure_codes":     claim_elem.findtext("ProcedureCodes", ""),
            "network_status":      claim_elem.findtext("NetworkStatus", "UNKNOWN"),
            "_source_format":      "xml_v1",
        })

    if claims:
        s3_key = f"hospital/{hospital_name}/{date_str}/claims_v1.json"
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=s3_key,
            Body=json.dumps({"hospital": hospital_name, "api_version": "v1",
                             "fetched_at": datetime.utcnow().isoformat(), "claims": claims}),
            ContentType="application/json", ServerSideEncryption="AES256",
        )
    return len(claims)


def fetch_all_hospitals(s3_bucket: str, prefix: str) -> dict:
    """Fetch from all hospital networks. Called by Airflow task."""
    since = datetime.utcnow() - timedelta(hours=4)
    results = {}
    for hospital_name, config in HOSPITAL_CONFIGS.items():
        try:
            if config["version"] == "v2":
                count = fetch_hospital_v2(hospital_name, config, since)
            else:
                count = fetch_hospital_v1(hospital_name, config, since)
            results[hospital_name] = {"status": "success", "count": count, "version": config["version"]}
        except Exception as e:
            logger.error(f"{hospital_name} fetch failed: {e}")
            results[hospital_name] = {"status": "failed", "error": str(e)}
    return results


def _get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
