# Automated-GCP-Cost-Anomaly-Report
This repository is used to Automated GCP Cost Anomaly Report


# GCP Cost Anomaly Detector

Daily GitHub Actions job that queries GCP Billing Export in BigQuery and notifies Slack and/or creates a GitHub issue if anomalous spend is detected.

## Setup
1. Enable Billing Export to BigQuery.
2. Add secrets: `GCP_SERVICE_ACCOUNT_KEY` (or configure Workload Identity) and `SLACK_WEBHOOK_URL`.
3. Edit `.github/workflows/detect-cost-anomalies.yml` to set `BILLING_TABLE`.
4. Push & run the workflow.

## Tuning
Adjust `THRESHOLD_PERCENT`, `BASELINE_DAYS`, and `MIN_ABSOLUTE_INCREASE` as needed.
