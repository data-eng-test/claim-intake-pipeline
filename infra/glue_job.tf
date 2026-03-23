# glue_job.tf
# Terraform configuration for the claim-intake Glue normalisation job.

resource "aws_glue_job" "claim_normalise" {
  name     = "claim-normalise-glue-job"
  role_arn = aws_iam_role.claims_glue_execution.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/claim_normalise.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 10
  worker_type       = "G.2X"
  timeout           = 120  # minutes
  max_retries       = 1

  default_arguments = {
    "--job-language"         = "python"
    "--enable-metrics"       = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--cloudwatch-log-group" = "/aws-glue/jobs/claim-normalise"
    "--s3_raw_bucket"        = aws_s3_bucket.claims_raw.bucket
    "--s3_processed_bucket"  = aws_s3_bucket.claims_processed.bucket
    "--TempDir"              = "s3://${aws_s3_bucket.glue_temp.bucket}/tmp/"
  }

  tags = {
    Application = "claim-intake-pipeline"
    Environment = "prod"
    Owner       = "claims-data-eng"
    CostCentre  = "DATA-ENG-001"
  }
}

resource "aws_s3_bucket" "claims_raw" {
  bucket = "insurer-claims-raw"
  tags = {
    Application = "claim-intake-pipeline"
    DataClass   = "PHI-PII"
  }
}

resource "aws_s3_bucket_versioning" "claims_raw_versioning" {
  bucket = aws_s3_bucket.claims_raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "claims_raw_lifecycle" {
  bucket = aws_s3_bucket.claims_raw.id
  rule {
    id     = "raw-claims-90-day-expiry"
    status = "Enabled"
    expiration { days = 90 }
  }
}

resource "aws_s3_bucket" "claims_processed" {
  bucket = "insurer-claims-processed"
  tags = {
    Application = "claim-intake-pipeline"
    DataClass   = "PHI-PII"
  }
}

resource "aws_iam_role" "claims_glue_execution" {
  name = "claims-glue-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = {
    Application = "claim-intake-pipeline"
  }
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "claims-glue-s3-policy"
  role = aws_iam_role.claims_glue_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::insurer-claims-raw/*",
          "arn:aws:s3:::insurer-claims-processed/*",
          "arn:aws:s3:::insurer-glue-scripts/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = ["arn:aws:secretsmanager:us-east-1:123456789:secret:claims/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:us-east-1:123456789:log-group:/aws-glue/*"]
      }
    ]
  })
}
