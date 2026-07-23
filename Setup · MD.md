# CloudWatch Logs & Metrics → Splunk Observability Cloud — Setup Guide

End-to-end guide for shipping CloudWatch logs and metrics to **Splunk Observability Cloud** via GitHub Actions workflows, with state stored in `state.json` / `metrics-state.json` in the repo.

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
        │       ├── Push to Splunk HEC (HTTP Event Collector)
        │       └── Update state.json with new timestamps
        ├── Run forward-metrics.py
        │       ├── Discover S3, DynamoDB, Lambda, API Gateway resources
        │       ├── Fetch CloudWatch metrics
        │       ├── Push to Splunk SignalFx ingest API
        │       └── Update metrics-state.json
        └── Commit updated state files back to repo
```

**Cost: $0 forever.** Splunk Observability Cloud Free Edition, no credit card, no expiry.

---

## Endpoint Reference

| Signal | URL | Auth |
|--------|-----|------|
| Logs | `https://http-inputs-<SPLUNK_HOST>/services/collector/event` | `Authorization: Splunk <HEC_TOKEN>` |
| Metrics | `https://ingest.<REALM>.signalfx.com/v2/datapoint` | `X-SF-Token: <INGEST_TOKEN>` |

Your `<REALM>` is shown in Settings → your username → Organizations (e.g. `us0`, `us1`, `eu0`).

---

## Prerequisites

| Tool | Min version |
|------|-------------|
| AWS CLI | 2.x |
| Python | 3.12 |
| GitHub Actions runner | ubuntu-latest |

---
sg0
## Step 1 — Sign up for Splunk Observability Cloud

1. Go to https://www.splunk.com/en_us/products/observability-cloud.html
2. Click **Get started free** → **Free Edition**
3. No credit card required
4. After login, note your **realm**: Settings → your username → Organizations section (e.g. `us0`, `eu0`)

---

## Step 2 — Create Ingest Token (Metrics)

1. In Splunk Observability Cloud, go to **Settings** → **Access Tokens**
2. Click **Create Token**
   - Name: `github-actions-ingest`
   - Scope: **Ingest**
3. Copy the token value
4. Save as GitHub secret: `SPLUNK_INGEST_TOKEN`

---
aYaduqQ0aqK3rcTXp5A4OA
## Step 3 — Set up Splunk Cloud for Logs (HEC)

Splunk Observability Cloud free edition bundles a Splunk Cloud instance for log ingestion.

### 3.1 Provision Splunk Cloud

1. In Splunk Observability Cloud, go to **Settings** → **Log Observer** → **Connect to Splunk Cloud**
2. Follow the wizard — it provisions your Splunk Cloud instance
3. Once provisioned, you'll see a button to **Open Splunk Cloud** or a URL like `https://prd-p-xxxxxx.splunkcloud.com`

### 3.2 Create HEC Token in Splunk Cloud

1. Open your Splunk Cloud instance URL (e.g. `https://prd-p-xxxxxx.splunkcloud.com`)
2. Log in with your Splunk credentials
3. In the top menu, go to **Settings** → **Data Inputs**
4. Click **HTTP Event Collector (HEC)**
5. If HEC is not enabled, click **Enable** to turn it on
6. Click **New Token** (or **Add** → **HTTP Event Collector**)
7. Fill in the form:
   - **Name:** `cloudwatch-logs`
   - **Source type:** `aws:cloudwatch`
   - **Index:** `main`
   - **Output group:** Default (or your preferred group)
8. **Important:** After creating the token, click on the token name to edit it:
   - Scroll to **Advanced Settings**
   - **Disable** "Indexer acknowledgement" (required for this integration)
   - Click **Save**
9. Copy the **Token** value (shown as a long string) → save as GitHub secret: `SPLUNK_HEC_TOKEN`
10. Note your Splunk Cloud hostname (format: `prd-p-xxxxxx.splunkcloud.com`) → save as GitHub secret: `SPLUNK_HEC_HOST`

**Alternative path if you can't find HEC:**
- In Splunk Cloud, use the search bar (top) and type "HTTP Event Collector"
- Or go to: **Apps** → **Search & Reporting** → **Settings** (gear icon) → **Data Inputs**

---

## Step 4 — AWS Preparation (OIDC)

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
  --role-name gha-cwlogs-splunk-deploy \
  --assume-role-policy-document file:///tmp/trust.json

# 3. Attach permissions
aws iam attach-role-policy \
  --role-name gha-cwlogs-splunk-deploy \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess

# Note the role ARN
aws iam get-role --role-name gha-cwlogs-splunk-deploy \
  --query Role.Arn --output text
```

---

## Step 5 — IAM Permissions

Attach additional permissions for Lambda and API Gateway:

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
  --policy-name gha-cwlogs-splunk-policy \
  --policy-document file:///tmp/splunk-policy.json

aws iam attach-role-policy \
  --role-name gha-cwlogs-splunk-deploy \
  --policy-arn arn:aws:iam::YOUR_ACCOUNT_ID:policy/gha-cwlogs-splunk-policy
```

---

## Step 6 — Repository Setup

### 6.1 GitHub Secrets (Settings → Secrets and variables → Actions)

| Secret | Value |
|--------|-------|
| `AWS_DEPLOY_ROLE_ARN` | ARN from Step 4 |
| `SPLUNK_HEC_TOKEN` | HEC token from Step 3 |
| `SPLUNK_HEC_HOST` | Splunk Cloud hostname from Step 3 |
| `SPLUNK_INGEST_TOKEN` | Ingest token from Step 2 |
| `SPLUNK_REALM` | Your realm (e.g. `us0`, `eu0`) |

### 6.2 GitHub Variables (Settings → Variables)

| Variable | Value |
|----------|-------|
| `AWS_REGION` | `us-east-1` (or your AWS region) |
| `LOG_GROUP_PREFIX` | (optional) Filter prefix for log groups |
| `BATCH_SIZE` | `500` |
| `LOOKBACK_HOURS` | `5` |

### 6.3 Push to main

```bash
git add .
git commit -m "Initial setup: CloudWatch → Splunk forwarder"
git push origin main
```

Once pushed:
- `forward-logs.yml` — Runs every 5 minutes via cron
- `forward-metrics.yml` — Runs every 5 minutes via cron
- Both are manually triggerable from the Actions tab

---

## Step 7 — Verify Data in Splunk

### Check Logs

1. Open **Splunk Cloud** → **Search**
2. Run: `index=main sourcetype="aws:cloudwatch"`
3. Should show events within 10 minutes of first workflow run
4. In **Splunk Observability Cloud** → **Log Observer** → same logs via Log Observer Connect

### Check Metrics

1. In **Splunk Observability Cloud**, go to **Metrics Finder**
2. Search: `aws.s3.BucketSizeBytes` or `aws.lambda.Invocations`
3. Should appear within 10 minutes of first workflow run

---

## Operational Reference

### Check last-fetch timestamps

```bash
cat state.json | jq .
cat metrics-state.json | jq .
```

### Manually trigger a run

From GitHub → **Actions** → **CloudWatch Logs → Splunk** or **CloudWatch Metrics → Splunk** → **Run workflow**

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

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| HEC returns 403 | Token disabled | Re-enable in Splunk Cloud → Settings → HEC |
| HEC returns 400 | Indexer acknowledgement is ON | Disable it in HEC token settings |
| Metrics not in Finder | Wrong `SPLUNK_REALM` | Check realm matches your org exactly |
| `X-SF-Token` rejected (401) | Token scope is `API` not `Ingest` | Create new token with scope **Ingest** |
| `lambda:ListFunctions` denied | Missing IAM permission | Add policy from Step 5 |
| No logs in Splunk Search | Wrong `SPLUNK_HEC_HOST` | Verify hostname matches Splunk Cloud instance |
| Duplicate events | Workflow running concurrently | `concurrency` block is already in workflow — ensure it's present |

---

## Security Notes

- Splunk tokens live **only** in GitHub secrets — never in code or repo files.
- The GHA deploy role uses **OIDC** — no long-lived AWS credentials stored in GitHub.
- `state.json` and `metrics-state.json` contain only resource names and timestamps — no sensitive data.