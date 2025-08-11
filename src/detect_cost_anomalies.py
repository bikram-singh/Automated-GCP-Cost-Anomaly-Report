#!/usr/bin/env python3
"""
Detect GCP cost anomalies from BigQuery billing export and
optionally post to Slack and create a GitHub issue.
Env vars:
  BILLING_TABLE (required) e.g. "project.dataset.table"
  THRESHOLD_PERCENT (optional, default 30)
  BASELINE_DAYS (optional, default 7)
  MIN_ABSOLUTE_INCREASE (optional, default 5.0)  # $ trigger for zero baseline
  SLACK_WEBHOOK_URL (optional)
  CREATE_GITHUB_ISSUE (optional, "true" to enable)
  GITHUB_TOKEN (required to create issue when CREATE_GITHUB_ISSUE=true)
  GITHUB_REPOSITORY (owner/repo) (optional; Actions sets it as github.repository)
"""
import os
import json
import logging
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BILLING_TABLE = os.getenv("BILLING_TABLE")
if not BILLING_TABLE:
    raise SystemExit("BILLING_TABLE environment variable is required (project.dataset.table)")

THRESHOLD_PERCENT = float(os.getenv("THRESHOLD_PERCENT", "30"))
BASELINE_DAYS = int(os.getenv("BASELINE_DAYS", "7"))
MIN_ABSOLUTE_INCREASE = float(os.getenv("MIN_ABSOLUTE_INCREASE", "5.0"))
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")
CREATE_ISSUE = os.getenv("CREATE_GITHUB_ISSUE", "false").lower() == "true"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY")  # owner/repo

client = bigquery.Client()

def run_query(yesterday, baseline_days):
    start_baseline = (yesterday - timedelta(days=baseline_days)).isoformat()
    y_date = yesterday.isoformat()

    query = f"""
    WITH baseline AS (
      SELECT service.description AS service, SUM(cost) AS baseline_total
      FROM `{BILLING_TABLE}`
      WHERE DATE(usage_start_time) >= DATE('{start_baseline}')
        AND DATE(usage_start_time) < DATE('{y_date}')
      GROUP BY service
    ),
    recent AS (
      SELECT service.description AS service, SUM(cost) AS recent_cost
      FROM `{BILLING_TABLE}`
      WHERE DATE(usage_start_time) = DATE('{y_date}')
      GROUP BY service
    )
    SELECT r.service, r.recent_cost, COALESCE(b.baseline_total, 0) AS baseline_total
    FROM recent r
    LEFT JOIN baseline b USING(service)
    ORDER BY r.recent_cost DESC
    """
    logging.info("Running BigQuery query for %s (baseline_days=%d)", y_date, baseline_days)
    job = client.query(query)
    return list(job.result())

def detect_anomalies(rows, baseline_days, threshold_pct, min_abs):
    anomalies = []
    for row in rows:
        service = row["service"]
        recent_cost = float(row["recent_cost"] or 0.0)
        baseline_total = float(row["baseline_total"] or 0.0)
        baseline_avg = baseline_total / baseline_days if baseline_days > 0 else 0.0

        if baseline_avg <= 0:
            # No baseline activity — treat as anomaly if recent exceeds absolute threshold
            if recent_cost >= min_abs:
                anomalies.append({
                    "service": service,
                    "recent_cost": recent_cost,
                    "baseline_avg": baseline_avg,
                    "percent_change": None,
                    "reason": f"no baseline; recent >= ${min_abs:.2f}"
                })
        else:
            pct_change = (recent_cost - baseline_avg) / baseline_avg * 100.0
            if pct_change > threshold_pct:
                anomalies.append({
                    "service": service,
                    "recent_cost": recent_cost,
                    "baseline_avg": baseline_avg,
                    "percent_change": pct_change,
                    "reason": f">{threshold_pct}%"
                })
    return anomalies

def post_to_slack(text):
    if not SLACK_WEBHOOK:
        logging.warning("SLACK_WEBHOOK_URL not set — skipping Slack post.")
        return False
    payload = {"text": text}
    try:
        r = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        r.raise_for_status()
        logging.info("Slack notification sent.")
        return True
    except Exception as e:
        logging.error("Failed to send Slack message: %s", e)
        return False

def create_github_issue(title, body):
    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        logging.warning("GITHUB_TOKEN or GITHUB_REPOSITORY not set — skipping issue creation.")
        return False
    url = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/issues"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    payload = {"title": title, "body": body}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        logging.info("GitHub issue created: %s", r.json().get("html_url"))
        return True
    except Exception as e:
        logging.error("Failed to create GitHub issue: %s", e)
        return False

def format_message(anomalies, y_date):
    lines = [f"*GCP Cost Anomalies for {y_date}* — {len(anomalies)} found\n"]
    for a in anomalies:
        pct = f"{a['percent_change']:.1f}%" if a['percent_change'] is not None else "N/A"
        lines.append(f"*Service:* {a['service']}\n  - Recent: ${a['recent_cost']:.2f}\n  - Baseline avg/day: ${a['baseline_avg']:.2f}\n  - Change: {pct}\n  - Note: {a['reason']}\n")
    return "\n".join(lines)

def main():
    yesterday = (datetime.utcnow().date() - timedelta(days=1))
    rows = run_query(yesterday, BASELINE_DAYS)
    anomalies = detect_anomalies(rows, BASELINE_DAYS, THRESHOLD_PERCENT, MIN_ABSOLUTE_INCREASE)

    if not anomalies:
        logging.info("No anomalies detected for %s", yesterday.isoformat())
        return

    message = format_message(anomalies, yesterday.isoformat())
    logging.info("Anomalies detected:\n%s", message)

    posted = post_to_slack(message)
    if CREATE_ISSUE:
        title = f"[Cost Anomaly] {len(anomalies)} anomaly(s) on {yesterday.isoformat()}"
        body = message + "\n\nDetected by automated job."
        created = create_github_issue(title, body)
        if not created:
            logging.warning("Issue creation requested but failed.")

if __name__ == "__main__":
    main()
