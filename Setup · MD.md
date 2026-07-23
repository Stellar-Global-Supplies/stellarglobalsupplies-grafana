# CloudWatch Logs & Metrics → New Relic — Setup Guide

End-to-end guide for shipping CloudWatch logs and metrics to **New Relic** via GitHub Actions workflows, with state stored in `state.json` / `metrics-state.json` in the repo.

**No CloudFormation. No AWS integration. Pure push from GitHub Actions only.**

---

## Architecture Overview

```
GitHub Actions (cron-based, OIDC auth)
        │
        ├── OIDC → Assume IAM Role
        ├── Run forwarder.py
        │       ├── Discover all CloudWatch log groups
        │       ├── Fetch events since last run (from state.json)
        │       ├── Push to New Relic Log API (HTTP POST JSON)
        │       └── Update state.json with new timestamps
        ├── Run forward-metrics.py
        │       ├── Discover S3, DynamoDB, Lambda, API Gateway resources
        │       ├── Fetch CloudWatch metrics
        │       ├── Push to New Relic Metric API (HTTP POST JSON)
        │       └── Update metrics-state.json
        └── Commit updated state files back to repo
```

**Cost: $0 forever.** New Relic Free Edition — 100 GB/month ingest, no credit card, no expiry.

---

## Endpoint Reference

| Signal  | URL                                          | Auth Header             |
|---------|----------------------------------------------|-------------------------|
| Logs    | `https://log-api.newrelic.com/log/v1`        | `Api-Key: <LICENSE_KEY>` |
| Metrics | `https://metric-api.newrelic.com/metric/v1`  | `Api-Key: <LICENSE_KEY>` |

> **EU accounts:** use `log-api.eu.newrelic.com` and `metric-api.eu.newrelic.com` and set the `NEW_RELIC_LOGS_URL` / `NEW_RELIC_METRICS_URL` GitHub Variables accordingly.

---

## Prerequisites

| Tool                | Min version   |
|---------------------|---------------|
| AWS CLI             | 2.x           |
| Python              | 3.12          |
| GitHub Actions runner | ubuntu-latest |

---

## Step 1 — Sign up for New Relic

1. Go to https://newrelic.com/signup
2. Fill in the form — **no credit card required**
3. Choose **US** or **EU** data centre (you can't change this later)
4. After login you land on the New Relic home page

Free tier includes:
- 100 GB/month ingest (logs + metrics combined)
- 1 full platform user
- Unlimited basic (read-only) users
- Full dashboard and alerting access

---

## Step 2 — Get your License Key

The license key is the single credential used for both logs and metrics.

1. In New Relic, click your **user icon** (bottom-left) → **API Keys**
2. Find the key of type **INGEST - LICENSE** (it starts with `NRAK-...`)
3. If none exists, click **Create a key** → Type: **Ingest - License**
4. Copy the key value
5. Save as GitHub secret: `NEW_RELIC_LICENSE_KEY`

---

## Step 3 — AWS Preparation (OIDC)

This lets GitHub Actions assume an IAM role without long-lived keys.

```bash
# 1. Create OIDC provider (one-time per account)
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

# 2. Create the deploy role
cat > /tmp/trust.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::YOUR_ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
      },
      "StringLike": {
        "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:*"
      }
    }
  }]
}
EOF

aws iam create-role \
  --role-name gha-cwlogs-newrelic-deploy \
  --assume-role-policy-document file:///tmp/trust.json

# 3. Attach permissions
aws iam attach-role-policy \
  --role-name gha-cwlogs-newrelic-deploy \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess

# Note the role ARN
aws iam get-role --role-name gha-cwlogs-newrelic-deploy \
  --query Role.Arn --output text
```

---

## Step 4 — IAM Permissions

Attach additional permissions for all services being monitored:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricStatistics",
        "logs:DescribeLogGroups",
        "logs:FilterLogEvents",
        "s3:ListAllMyBuckets",
        "dynamodb:ListTables",
        "lambda:ListFunctions",
        "apigateway:GET"
      ],
      "Resource": "*"
    }
  ]
}
```

```bash
aws iam create-policy \
  --policy-name gha-cwlogs-newrelic-policy \
  --policy-document file:///tmp/newrelic-policy.json

aws iam attach-role-policy \
  --role-name gha-cwlogs-newrelic-deploy \
  --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/gha-cwlogs-newrelic-policy
```

---

## Step 5 — Repository Setup

### 5.1 GitHub Secrets (Settings → Secrets and variables → Actions)

| Secret                  | Value                          |
|-------------------------|--------------------------------|
| `AWS_DEPLOY_ROLE_ARN`   | ARN from Step 3                |
| `NEW_RELIC_LICENSE_KEY` | License key from Step 2        |

### 5.2 GitHub Variables (Settings → Variables)

| Variable              | Value                                              |
|-----------------------|----------------------------------------------------|
| `AWS_REGION`          | `us-east-1` (or your AWS region)                  |
| `LOG_GROUP_PREFIX`    | (optional) Filter prefix for log groups            |
| `BATCH_SIZE`          | `500`                                              |
| `LOOKBACK_HOURS`      | `5`                                                |
| `NEW_RELIC_LOGS_URL`  | `https://log-api.newrelic.com/log/v1` *(EU: `https://log-api.eu.newrelic.com/log/v1`)* |
| `NEW_RELIC_METRICS_URL` | `https://metric-api.newrelic.com/metric/v1` *(EU: `https://metric-api.eu.newrelic.com/metric/v1`)* |

> `NEW_RELIC_LOGS_URL` and `NEW_RELIC_METRICS_URL` are optional — scripts default to US endpoints. Only set them if you're on the EU data centre.

### 5.3 Push to main

```bash
git add .
git commit -m "chore: switch forwarder target from Splunk to New Relic"
git push origin main
```

Once pushed:
- `forward-logs.yml` — Runs every 5 minutes via cron
- `forward-metrics.yml` — Runs every 5 minutes via cron

---

## Step 6 — Verify Data in New Relic

### Check Logs

1. In New Relic, go to **Logs** (left sidebar)
2. Search: `log_group:*` or filter by `source: aws:cloudwatch`
3. Logs should appear within 5–10 minutes of first workflow run

### Check Metrics

1. In New Relic, go to **Query Your Data** (top nav, looks like a chart icon)
2. Run an NRQL query:
   ```sql
   SELECT latest(value) FROM Metric
   WHERE metricName LIKE 'aws.lambda%'
   FACET function_name
   SINCE 1 hour ago
   ```
3. Or for S3:
   ```sql
   SELECT latest(value) FROM Metric
   WHERE metricName = 'aws.s3.BucketSizeBytes.Average'
   FACET bucket
   SINCE 1 day ago
   ```

---

## Metric Naming Reference

All metrics pushed use the pattern `aws.<service>.<MetricName>.<Stat>`:

| Service     | Example metric name                              |
|-------------|--------------------------------------------------|
| S3          | `aws.s3.BucketSizeBytes.Average`                 |
| S3          | `aws.s3.4xxErrors.Sum`                           |
| DynamoDB    | `aws.dynamodb.ConsumedReadCapacityUnits.Sum`      |
| DynamoDB    | `aws.dynamodb.SuccessfulRequestLatency.Average`   |
| Lambda      | `aws.lambda.Invocations.Sum`                     |
| Lambda      | `aws.lambda.Duration.Average`                    |
| Lambda      | `aws.lambda.Errors.Sum`                          |
| API Gateway | `aws.apigateway.Count.Sum`                       |
| API Gateway | `aws.apigateway.Latency.Average`                 |

---

## Operational Reference

### Check last-fetch timestamps

```bash
cat state.json | jq .
cat metrics-state.json | jq .
```

### Manually trigger a run

GitHub → **Actions** → **CloudWatch Logs → New Relic** or **CloudWatch Metrics → New Relic** → **Run workflow**

### Reset state (force re-send)

```bash
echo '{}' > state.json
echo '{}' > metrics-state.json
git commit -am "chore: reset state [skip ci]"
git push
```

### Change schedule frequency

Edit `.github/workflows/forward-logs.yml` or `forward-metrics.yml`:

```yaml
schedule:
  - cron: "*/5 * * * *"   # every 5 minutes
  - cron: "0 * * * *"     # every hour
```

---

## Troubleshooting

| Symptom                          | Likely cause                        | Fix                                              |
|----------------------------------|-------------------------------------|--------------------------------------------------|
| Log API returns 403              | Wrong or expired license key        | Re-generate key in New Relic → API Keys          |
| Log API returns 413              | Batch too large                     | Reduce `BATCH_SIZE` to `200`                     |
| Metric API returns 400           | Malformed payload or bad value type | Check logs for the exact metric name + value     |
| No logs in New Relic Logs        | Wrong data centre (US vs EU)        | Set `NEW_RELIC_LOGS_URL` to EU endpoint          |
| No metrics in Query Your Data    | Wrong data centre                   | Set `NEW_RELIC_METRICS_URL` to EU endpoint       |
| `lambda:ListFunctions` denied    | Missing IAM permission              | Add policy from Step 4                           |
| Duplicate events                 | Workflow running concurrently       | Ensure `concurrency` block is present in workflow|
| 100 GB limit hit mid-month       | Too much log data                   | Add `LOG_GROUP_PREFIX` to filter noisier groups  |

---

## Security Notes

- New Relic license key lives **only** in GitHub secrets — never in code or repo files.
- The GHA deploy role uses **OIDC** — no long-lived AWS credentials stored in GitHub.
- `state.json` and `metrics-state.json` contain only resource names and timestamps — no sensitive data.