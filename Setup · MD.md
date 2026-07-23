# CloudWatch Logs & Metrics → New Relic — Setup Guide

End-to-end guide for shipping every CloudWatch log group and key CloudWatch metrics (S3, DynamoDB) to **New Relic** via GitHub Actions workflows, with state stored in `state.json` / `metrics-state.json` in the repo.

---

## Architecture Overview

```
GitHub Actions (cron-based)
        │
        ├── OIDC → Assume IAM Role
        ├── Read NEW_RELIC_LICENSE_KEY from GitHub secrets
        ├── Run forwarder.py
        │       ├── Discover all CloudWatch log groups
        │       ├── Fetch events since last run (from state.json)
        │       ├── Push to New Relic Logs API
        │       └── Update state.json with new timestamps
        ├── Run forward-metrics.py
        │       ├── Discover S3 buckets & DynamoDB tables
        │       ├── Fetch CloudWatch metrics
        │       ├── Push to New Relic Metrics API
        │       └── Update metrics-state.json
        └── Commit updated state files back to repo
```

**Key design decisions**

| Concern | Decision |
|---|---|
| State | `state.json` / `metrics-state.json` committed to repo after each run |
| Scheduling | GitHub Actions cron (logs: hourly, metrics: daily) |
| Auth | New Relic Ingest License Key stored in GitHub secret; never in code |
| Retries | Failed log groups keep old timestamp in state → retried next run |
| Idempotency | New Relic deduplicates by `timestamp + message` |

---

## Prerequisites

| Tool | Min version |
|---|---|
| AWS CLI | 2.x |
| Python | 3.12 |
| GitHub Actions runner | ubuntu-latest |

---

## Step 1 — New Relic Setup

### 1.1 Create a New Relic account

1. Sign up at <https://newrelic.com/signup>
2. Free tier includes **100 GB/month** of data ingestion — sufficient for moderate CloudWatch volume

### 1.2 Find your Ingest License Key

1. Log in to [New Relic One](https://one.newrelic.com)
2. Go to **Settings** (gear icon) → **API Keys**
3. Click **Create a key**
   - Key type: **INGEST** (not USER)
   - Name: `cloudwatch-forwarder`
4. Copy the key value (shown only once)
5. **Important:** Note your New Relic **region**:
   - **US** region: `log-api.newrelic.com` / `metric-api.newrelic.com`
   - **EU** region: `log-api.eu.newrelic.com` / `metric-api.eu.newrelic.com`

---

## Step 2 — AWS Preparation

### 2.1 Create OIDC trust for GitHub Actions

This lets GHA assume an IAM role without long-lived keys.

```bash
# 1. Create OIDC provider (one-time per account)
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

# 2. Create the deploy role (update placeholders)
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

# 3. Attach necessary permissions (minimal set)
aws iam attach-role-policy \
  --role-name gha-cwlogs-newrelic-deploy \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess   # tighten as needed

# Note the role ARN — you'll need it for GitHub secrets
aws iam get-role --role-name gha-cwlogs-newrelic-deploy \
  --query Role.Arn --output text
```

---

## Step 3 — Repository Setup

### 3.1 Repository secrets (Settings → Secrets and variables → Actions)

| Secret | Value |
|---|---|
| `AWS_DEPLOY_ROLE_ARN` | ARN from Step 2.1 |
| `NEW_RELIC_LICENSE_KEY` | New Relic Ingest License Key from Step 1.2 |

### 3.2 Repository variables (Settings → Variables)

| Variable | Value |
|---|---|
| `AWS_REGION` | `us-east-1` (or your AWS region) |
| `NEW_RELIC_REGION` | `eu` (or `us` if using US region) |

### 3.3 Push to main

```bash
git add .
git commit -m "Initial setup: forwarder and workflows"
git push origin main
```

Once pushed, the workflows will:
- `forward-logs.yml` — Run automatically every hour via cron
- `forward-metrics.yml` — Run automatically once a day at 7:00 AM
- Both are manually triggerable from the Actions tab

---

## Step 4 — IAM Permissions (for metrics)

The OIDC deploy role needs these additional permissions to fetch S3 and DynamoDB metrics:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:ListTables"
      ],
      "Resource": "*"
    }
  ]
}
```

Attach them via the AWS CLI:

```bash
# Create a policy from the JSON above
aws iam create-policy \
  --policy-name gha-cwlogs-metrics-policy \
  --policy-document file:///tmp/metrics-policy.json

# Attach to the existing role
aws iam attach-role-policy \
  --role-name gha-cwlogs-newrelic-deploy \
  --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/gha-cwlogs-metrics-policy
```

---

## Step 5 — Verify in New Relic

### CloudWatch Logs

1. Open [New Relic One](https://one.newrelic.com)
2. Go to **Logs** → **Logs Management**
3. Run a NRQL query:
```sql
SELECT * FROM Log WHERE job = 'cloudwatch-forwarder' SINCE 1 hour ago
```
4. Filter by log group:
```sql
SELECT * FROM Log WHERE log_group = '/aws/lambda/my-function' SINCE 1 hour ago
```

### CloudWatch Metrics (S3 & DynamoDB)

1. Go to **Metrics** → **Metrics Explorer**
2. Query by metric name:
```sql
SELECT average(custom.cloudwatch.s3.BucketSizeBytes) FROM Metric SINCE 1 day ago TIMESERIES
```
3. Filter by source:
```sql
SELECT average(custom.cloudwatch.s3.BucketSizeBytes) FROM Metric WHERE source = 's3' SINCE 1 day ago TIMESERIES
```
4. DynamoDB example:
```sql
SELECT sum(custom.cloudwatch.dynamodb.ConsumedReadCapacityUnits) FROM Metric WHERE source = 'dynamodb' SINCE 1 day ago TIMESERIES
```

### View all forwarded metrics

```sql
SELECT uniques(metricName) FROM Metric WHERE metricName LIKE 'custom.cloudwatch.%' SINCE 1 day ago
```

---

## Step 6 — Create Dashboards in New Relic

### 6.1 CloudWatch Logs Dashboard

1. In [New Relic One](https://one.newrelic.com), go to **Dashboards** → **Create a dashboard**
2. Name: `CloudWatch Logs Overview`
3. Add widgets using NRQL queries:

**Widget 1 — Log Volume by Log Group (bar chart)**
```sql
SELECT count(*) FROM Log WHERE job = 'cloudwatch-forwarder' FACET log_group SINCE 1 day ago TIMESERIES
```
- Visualization: **Stacked bar**

**Widget 2 — Logs by Region (pie chart)**
```sql
SELECT count(*) FROM Log WHERE job = 'cloudwatch-forwarder' FACET region SINCE 1 day ago
```
- Visualization: **Pie**

**Widget 3 — Recent Errors (table)**
```sql
SELECT timestamp, message, log_group FROM Log WHERE job = 'cloudwatch-forwarder' AND message LIKE '%error%' OR message LIKE '%ERROR%' OR message LIKE '%Exception%' SINCE 1 hour ago
```
- Visualization: **Table**

**Widget 4 — Log Ingestion Rate (line chart)**
```sql
SELECT rate(count(*), 1 minute) FROM Log WHERE job = 'cloudwatch-forwarder' SINCE 1 day ago TIMESERIES
```
- Visualization: **Line**

### 6.2 S3 Metrics Dashboard

1. **Dashboards** → **Create a dashboard**
2. Name: `S3 CloudWatch Metrics`

**Widget 1 — Bucket Storage Size (line chart)**
```sql
SELECT average(custom.cloudwatch.s3.BucketSizeBytes) FROM Metric FACET bucket SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

**Widget 2 — Object Count (line chart)**
```sql
SELECT average(custom.cloudwatch.s3.NumberOfObjects) FROM Metric FACET bucket SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

**Widget 3 — Request Volume (line chart)**
```sql
SELECT sum(custom.cloudwatch.s3.GetObject) + sum(custom.cloudwatch.s3.PutObject) + sum(custom.cloudwatch.s3.ListBucket) FROM Metric FACET bucket SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

**Widget 4 — Error Rate (billboard)**
```sql
SELECT sum(custom.cloudwatch.s3.4xxErrors) + sum(custom.cloudwatch.s3.5xxErrors) FROM Metric SINCE 7 days ago
```
- Visualization: **Billboard**

### 6.3 DynamoDB Metrics Dashboard

1. **Dashboards** → **Create a dashboard**
2. Name: `DynamoDB CloudWatch Metrics`

**Widget 1 — Read/Write Capacity (line chart)**
```sql
SELECT average(custom.cloudwatch.dynamodb.ConsumedReadCapacityUnits) AS 'Read (CU)', average(custom.cloudwatch.dynamodb.ConsumedWriteCapacityUnits) AS 'Write (CU)' FROM Metric FACET table SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

**Widget 2 — Throttled Requests (line chart)**
```sql
SELECT sum(custom.cloudwatch.dynamodb.ThrottledRequests) FROM Metric FACET table SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

**Widget 3 — System & User Errors (line chart)**
```sql
SELECT sum(custom.cloudwatch.dynamodb.SystemErrors) AS 'System Errors', sum(custom.cloudwatch.dynamodb.UserErrors) AS 'User Errors' FROM Metric SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

**Widget 4 — Request Latency (line chart)**
```sql
SELECT average(custom.cloudwatch.dynamodb.SuccessfulRequestLatency) FROM Metric FACET table SINCE 7 days ago TIMESERIES
```
- Visualization: **Line**

### 6.4 Combined CloudWatch Overview Dashboard

Create a single dashboard with all the key widgets:

1. **Dashboards** → **Create a dashboard**
2. Name: `CloudWatch Overview`

**Widget 1 — Log Ingestion Rate**
```sql
SELECT rate(count(*), 1 minute) FROM Log WHERE job = 'cloudwatch-forwarder' SINCE 1 day ago TIMESERIES
```

**Widget 2 — Top 10 Log Groups by Volume**
```sql
SELECT count(*) FROM Log WHERE job = 'cloudwatch-forwarder' FACET log_group LIMIT 10 SINCE 1 day ago
```

**Widget 3 — S3 Bucket Sizes**
```sql
SELECT latest(custom.cloudwatch.s3.BucketSizeBytes) FROM Metric FACET bucket SINCE 1 day ago
```

**Widget 4 — DynamoDB Throttling Alerts**
```sql
SELECT sum(custom.cloudwatch.dynamodb.ThrottledRequests) FROM Metric WHERE throttledRequests > 0 FACET table SINCE 1 day ago
```

### 6.5 Set up Alerts (optional)

1. Go to **Alerts & AI** → **Alert conditions (NRQL)**
2. Create a condition:
   - Name: `High DynamoDB Throttling`
   - NRQL: `SELECT sum(custom.cloudwatch.dynamodb.ThrottledRequests) FROM Metric SINCE 5 minutes ago`
   - Threshold: `> 10` for `5 minutes`
   - Policy: Create new or add to existing

3. Create another:
   - Name: `S3 High Error Rate`
   - NRQL: `SELECT sum(custom.cloudwatch.s3.4xxErrors) + sum(custom.cloudwatch.s3.5xxErrors) FROM Metric SINCE 5 minutes ago`
   - Threshold: `> 50` for `5 minutes`

---

## Operational Reference

### Check last-fetch timestamps in state files

```bash
cat state.json | jq .
cat metrics-state.json | jq .
```

Example output:
```json
{
  "/aws/lambda/my-function": {
    "lastFetchMs": 1721678400000,
    "updatedAt": "2026-07-22T23:00:00+00:00"
  }
}
```

### Manually trigger a run

From GitHub → **Actions** → **Forward CloudWatch Logs to New Relic** → **Run workflow**

### Reset a log group's last-fetch time (force re-send)

Delete the entry from `state.json` locally, commit, and push:
```bash
# Remove a specific log group entry
jq 'del(.["/aws/lambda/my-function"])' state.json > state.json.tmp && mv state.json.tmp state.json

# Or reset entire state to re-send everything on next run
echo '{}' > state.json

git commit -am "chore: reset forwarder state [skip ci]"
git push
```

On the next run, it will look back `LOOKBACK_HOURS` (default 5) and re-send.

### View workflow logs

From GitHub → **Actions** → click the latest run → expand steps to see output.

### Change schedule frequency

Edit `.github/workflows/forward-logs.yml` → change the cron expression:

```yaml
schedule:
  - cron: "*/30 * * * *"   # every 30 minutes
  - cron: "0 */2 * * *"   # every 2 hours
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Workflow fails at OIDC step | Trust policy org/repo mismatch | Verify `YOUR_ORG/YOUR_REPO` in trust policy |
| `HTTP 401` in workflow logs | Invalid or missing license key | Update `NEW_RELIC_LICENSE_KEY` secret in GitHub → re-run |
| `HTTP 403` in workflow logs | License key doesn't have INGEST type | Create a new key with type **INGEST** in New Relic |
| `HTTP 429` in workflow logs | Rate-limited by New Relic | Reduce `BATCH_SIZE` via `BATCH_SIZE` variable |
| State not updating | Workflow error mid-run | Check workflow logs; failed groups keep old timestamp |
| No data in New Relic | Wrong region endpoint | Set `NEW_RELIC_REGION` to `eu` or `us` correctly |
| No data in New Relic | License key is USER type instead of INGEST | Create a new INGEST key in New Relic API Keys |
| `AccessDenied` on `logs:FilterLogEvents` | IAM role missing permissions | Attach `CloudWatchLogsReadOnlyAccess` to the role |

---

## Cost Estimate (rough)

| Service | Estimate |
|---|---|
| GitHub Actions | ~$0 (public repo: 2000 min/month free; private: also free tier) |
| CloudWatch API calls | ~$0.01–$0.10 depending on log volume |
| **New Relic** | Free tier: 100 GB/month logs + metrics ingested |

---

## Security Notes

- The New Relic Ingest License Key lives **only** in GitHub secrets — never in code or repo files.
- The GHA deploy role uses **OIDC** — no long-lived AWS credentials stored in GitHub.
- The IAM role should be **least-privilege**: read-only on CloudWatch Logs, plus the metrics permissions listed above.
- `state.json` and `metrics-state.json` contain only log group names, table/bucket names, and timestamps — no sensitive data.