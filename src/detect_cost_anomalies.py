import os
import json
import logging
import argparse
from datetime import datetime, timedelta
from google.cloud import bigquery
import requests

# ---------------- CONFIG ----------------
PROJECT_ID = os.getenv("PROJECT_ID", "my-billing-project")
DATASET_ID = os.getenv("DATASET_ID", "billing_dataset")
TABLE_ID = os.getenv("TABLE_ID", "gcp_billing_export_v1_01F182_446702_47B35A")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
BASELINE_DAYS = int(os.getenv("BASELINE_DAYS", "7"))
THRESHOLD_PERCENT = float(os.getenv("THRESHOLD_PERCENT", "50"))
MIN_ABSOLUTE_INCREASE = float(os.getenv("MIN_ABSOLUTE_INCREASE", "10"))
CREATE_ISSUE = os.getenv("CREATE_ISSUE", "false").lower() == "true"
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
# -----------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def run_query(yesterday, baseline_days):
    logging.info("Querying BigQuery for %s", yesterday)
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        WITH baseline AS (
          SELECT service.description AS service,
                 AVG(cost) AS avg_cost
          FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
          WHERE usage_start_time BETWEEN TIMESTAMP_SUB('{yesterday}', INTERVAL {baseline_days+1} DAY)
                                     AND TIMESTAMP_SUB('{yesterday}', INTERVAL 1 DAY)
          GROUP BY service
        ),
        recent AS (
          SELECT service.description AS service,
                 SUM(cost) AS recent_cost
          FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
          WHERE usage_start_time BETWEEN '{yesterday}' AND TIMESTAMP_ADD('{yesterday}', INTERVAL 1 DAY)
          GROUP BY service
        )
        SELECT r.service, r.recent_cost, b.avg_cost
        FROM recent r
        JOIN baseline b USING(service)
    """
    return list(client.query(query).result())

def detect_anomalies(rows, baseline_days, threshold_percent, min_absolute_increase):
    anomalies = []
    for row in rows:
        recent_cost = row["recent_cost"] if isinstance(row, dict) else row.recent_cost
        baseline_avg = row["avg_cost"] if isinstance(row, dict) else row.avg_cost
        if baseline_avg == 0:
            continue
        percent_change = ((recent_cost - baseline_avg) / baseline_avg) * 100
        if percent_change > threshold_percent and (recent_cost - baseline_avg) > min_absolute_increase:
            anomalies.append({
                "service": row["service"] if isinstance(row, dict) else row.service,
                "recent_cost": recent_cost,
                "baseline_avg": baseline_avg,
                "percent_change": percent_change,
                "reason": "Cost spike detected"
            })
    return anomalies

def format_message(anomalies, date_str):
    lines = [f"*Cost anomalies detected for {date_str}:*"]
    for a in anomalies:
        lines.append(
            f"- {a['service']}: ${a['recent_cost']:.2f} (avg: ${a['baseline_avg']:.2f}, change: {a['percent_change']:.1f}%) — {a['reason']}"
        )
    return "\n".join(lines)

def post_to_slack(message):
    if not SLACK_WEBHOOK_URL:
        logging.warning("No Slack webhook URL configured.")
        return False
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        r.raise_for_status()
        logging.info("Message posted to Slack.")
        return True
    except Exception as e:
        logging.error(f"Failed to post to Slack: {e}")
        return False

def create_github_issue(title, body):
    if not GITHUB_REPO or not GITHUB_TOKEN:
        logging.warning("GitHub repo or token not configured.")
        return False
    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    payload = {"title": title, "body": body}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
        logging.info("GitHub issue created.")
        return True
    except Exception as e:
        logging.error(f"Failed to create GitHub issue: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Send a dummy anomaly for testing without querying BigQuery.")
    args = parser.parse_args()

    yesterday = (datetime.utcnow().date() - timedelta(days=1))

    if args.test:
        logging.info("Running in TEST mode — skipping BigQuery query.")
        anomalies = [{
            "service": "Test Service",
            "recent_cost": 150.00,
            "baseline_avg": 10.00,
            "percent_change": 1400.0,
            "reason": "Test anomaly"
        }]
    else:
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
