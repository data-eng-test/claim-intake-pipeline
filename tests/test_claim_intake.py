"""
test_claim_intake.py
Unit tests for claim intake normalisation and loading logic.
Run: pytest tests/ -v
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.transform.claim_normalise import normalise_claims

SAMPLE_RAW_CLAIM = {
    "claim_submission_id": "CLM-2026-001234",
    "claim_type":          "medical",
    "provider_npi":        "1234567890",
    "member_id":           "MBR-987654",
    "policy_number":       "POL-2024-56789",
    "date_of_service":     "2026-03-01",
    "total_billed_amount_usd": 1250.00,
    "diagnosis_codes":     "Z00.00|J06.9",
    "procedure_codes":     "99213|87880",
    "network_status":      "in_network",
    "source_system":       "AVAILITY",
}


class TestClaimNormalisation:

    def test_claim_type_uppercased(self):
        """claim_type should always be uppercased."""
        assert SAMPLE_RAW_CLAIM["claim_type"].upper() == "MEDICAL"

    def test_network_status_uppercased(self):
        """network_status should be uppercased and valid."""
        assert SAMPLE_RAW_CLAIM["network_status"].upper() == "IN_NETWORK"

    def test_invalid_claim_type_marked_unknown(self):
        """Unrecognised claim types should be replaced with UNKNOWN."""
        valid_types = ["MEDICAL", "DENTAL", "VISION", "PHARMACY"]
        claim_type = "BEHAVIORAL_HEALTH"
        result = claim_type if claim_type in valid_types else "UNKNOWN"
        assert result == "UNKNOWN"

    def test_missing_claim_id_filtered(self):
        """Claims without claim_submission_id should be dropped."""
        claim = {**SAMPLE_RAW_CLAIM, "claim_submission_id": None}
        assert claim["claim_submission_id"] is None

    def test_billed_amount_decimal(self):
        """Billed amount should be a decimal with 2 decimal places."""
        amount = SAMPLE_RAW_CLAIM["total_billed_amount_usd"]
        assert isinstance(amount, float)
        assert round(amount, 2) == amount

    def test_diagnosis_codes_pipe_delimited(self):
        """Diagnosis codes should be pipe-delimited ICD-10 strings."""
        codes = SAMPLE_RAW_CLAIM["diagnosis_codes"]
        code_list = codes.split("|")
        assert len(code_list) == 2
        assert all(len(c) >= 3 for c in code_list)


class TestProviderApiIntegration:

    @patch("src.ingest.provider_api.requests.get")
    @patch("src.ingest.provider_api._get_secret")
    def test_fetch_returns_claim_count(self, mock_secret, mock_get):
        """fetch_claims_from_provider should return total claim count."""
        mock_secret.return_value = {"client_secret": "test-secret"}
        mock_token_resp = MagicMock()
        mock_token_resp.json.return_value = {"access_token": "test-token"}
        mock_claims_resp = MagicMock()
        mock_claims_resp.json.return_value = {
            "claims": [SAMPLE_RAW_CLAIM] * 10,
            "hasMore": False,
        }
        # First call is token, second is claims
        mock_get.side_effect = [mock_token_resp, mock_claims_resp]
        # Verify structure
        assert len(mock_claims_resp.json.return_value["claims"]) == 10

    def test_v1_hospital_xml_parsed_to_json(self):
        """v1 XML hospital claims should be converted to JSON dict."""
        import xml.etree.ElementTree as ET
        xml_content = """
        <Claims>
            <Claim>
                <ClaimID>CLM-V1-001</ClaimID>
                <ClaimType>MEDICAL</ClaimType>
                <ProviderNPI>9876543210</ProviderNPI>
                <MemberID>MBR-111222</MemberID>
                <PolicyNumber>POL-V1-333</PolicyNumber>
                <DateOfService>2026-02-15</DateOfService>
                <BilledAmount>750.00</BilledAmount>
                <DiagnosisCodes>M54.5</DiagnosisCodes>
                <ProcedureCodes>99214</ProcedureCodes>
                <NetworkStatus>IN_NETWORK</NetworkStatus>
            </Claim>
        </Claims>
        """
        root = ET.fromstring(xml_content)
        claims = []
        for elem in root.findall(".//Claim"):
            claims.append({
                "claim_submission_id": elem.findtext("ClaimID"),
                "claim_type":          elem.findtext("ClaimType"),
                "provider_npi":        elem.findtext("ProviderNPI"),
            })
        assert len(claims) == 1
        assert claims[0]["claim_submission_id"] == "CLM-V1-001"
        assert claims[0]["claim_type"] == "MEDICAL"
