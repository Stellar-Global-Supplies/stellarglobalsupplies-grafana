# Stellar Global Supplies — Grafana/New Relic Monitoring Stack

A comprehensive observability stack that collects metrics, logs, and cost data from AWS, CloudFront, and Supabase, and forwards everything to **New Relic** for visualization and alerting.

> **Cost: $0/month** — runs entirely on New Relic Free Edition (100 GB/month ingest) + GitHub Actions (free tier) + AWS OIDC (no long-lived credentials).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Pipelines](#pipelines)
   - [1. CloudFront Web Traffic](#1-cloudfront-web-traffic)
   - [2. AWS Cost & Usage Report (CUR)](#2-aws-cost--usage-report-cur)
   - [3. CloudWatch Logs](#3-cloudwatch-logs)
   - [4. CloudWatch Metrics](#4-cloudwatch-metrics)
   - [5. Supabase Postgres](#5-supabase-postgres)
3. [File Structure](#file-structure)
4. [Schedule Overview](#schedule-overview)
5. [Cost Analysis](#cost-analysis)
6. [Setup Guide](#setup-guide)
7. [Security](#security)
8. [Troubleshooting](#troubleshooting)
9. [Related Documentation](#related-documentation)

---

## Architecture Overview

```
GitHub Actions (cron-based, OIDC auth)
        │
        ├── OIDC → Assume IAM Role (no long-lived AWS keys)
        │
        ├── forward-web-traffic.yml (every 4 hours)
        │     ├── webtraffic_processor.py  → S3 (CloudFront logs)
        │     └── forward-web-traffic.py   → New Relic Metric API
        │
        ├── forward-cur.yml (every 8 hours)
        │     ├── cur_processor.py         → S3 (AWS CUR data)
        │     └── forward_cur.py           → New Relic Metric API
        │
        ├── forward-logs.yml (every hour)
        │     └── forwarder.py             → New Relic Log API
        │
        ├── forward-metrics.yml (daily @ 7 PM)
        │     └── forward-metrics.py       → New Relic Metric API
        │
        └── forward-supabase.yml (every hour)
              └── supabase/main.py         → New Relic Metric + Log API
```

All state (last-fetch timestamps, metrics state) is committed back to the `state` branch of this repository.

---

## Pipelines

### 1. CloudFront Web Traffic

Processes CloudFront access logs from S3 and forwards traffic analytics to New Relic.

| Component | File | Description |
|-----------|------|-------------|
| Processor | `webtraffic_processor.py` | Downloads `.gz` log files from S3, parses CloudFront JSON logs, extracts 15+ metrics, generates `web-traffic-report.json` (7-day rolling window) |
| Forwarder | `forward-web-traffic.py` | Reads the report, converts to 50+ New Relic metrics, pushes to New Relic Metric API under `aws.webtraffic.*` namespace |
| Workflow | `.github/workflows/forward-web-traffic.yml` | Runs every 4 hours, commits report to `state` branch |
| Dashboard | `dashboard.json` | 22-widget New Relic dashboard |

**Metrics collected:** Total requests, unique IPs, error rate, cache hit ratio, HTTPS usage, browser/OS distribution, device split, top pages, geo distribution, hourly patterns, response times, HTTP methods, referrers.

**Metric namespace:** `aws.webtraffic.*`

**Documentation:** [WEB_TRAFFIC_README.md](./WEB_TRAFFIC_README.md) | [docs/webtraffic_processor.md](./docs/webtraffic_processor.md) | [docs/forward-web-traffic.md](./docs/forward-web-traffic.md)

---

### 2. AWS Cost & Usage Report (CUR)

Downloads and processes AWS Cost and Usage Reports from S3, then forwards cost metrics to New Relic.

| Component | File | Description |
|-----------|------|-------------|
| Processor | `cur_processor.py` | Reads CUR manifest from S3, downloads/decompresses CSV data, transforms records, generates 5 JSON output files (costs, daily-costs, summary, costs-by-tag, costs-by-usage-group) |
| Forwarder | `forward_cur.py` | Reads the 5 JSON files, converts to New Relic metrics, pushes to Metric API under `aws.cur.*` namespace |
| Workflow | `.github/workflows/forward-cur.yml` | Runs every 8 hours, cleans up generated files after forwarding |

**Output files:**
- `costs.json` — granular daily cost per service per region
- `daily-costs.json` — daily total + per-service rollup
- `summary.json` — monthly total per service
- `costs-by-tag.json` — cost breakdown by application tag + uncategorized
- `costs-by-usage-group.json` — daily cost per service + normalized resource group

**Metric namespace:** `aws.cur.*`

**Documentation:** [docs/cur_processor.md](./docs/cur_processor.md) | [docs/forward_cur.md](./docs/forward_cur.md)

---

### 3. CloudWatch Logs

Discovers all CloudWatch log groups, fetches new events since the last run, and forwards them to New Relic's Log API.

| Component | File | Description |
|-----------|------|-------------|
| Forwarder | `forwarder.py` | Discovers all CW log groups, fetches events since last run (stored in `state.json`), pushes to New Relic Log API, updates state |
| Workflow | `.github/workflows/forward-logs.yml` | Runs every hour, restores `state.json` from `state` branch, commits updated state |

**Key features:**
- Incremental fetching via `state.json` (last-fetch timestamps per log group)
- 5-hour lookback on first run
- 10,000 event cap per log group per run
- Batching with configurable batch size (default: 500)

**Metric namespace:** `source: aws:cloudwatch` (log attribute)

**Documentation:** [docs/forwarder.md](./docs/forwarder.md)

---

### 4. CloudWatch Metrics

Discovers AWS resources (S3, DynamoDB, Lambda, API Gateway, Step Functions) and forwards their CloudWatch metrics to New Relic.

| Component | File | Description |
|-----------|------|-------------|
| Forwarder | `forward-metrics.py` | Discovers resources, fetches CloudWatch metrics with appropriate lookback windows, pushes to New Relic Metric API |
| Workflow | `.github/workflows/forward-metrics.yml` | Runs daily at 7 PM, restores `metrics-state.json` from `state` branch, commits updated state |

**Services monitored:**
- **S3** — BucketSizeBytes, NumberOfObjects (via direct ListObjectsV2), request metrics (AllRequests, GetRequests, PutRequests, 4xxErrors, 5xxErrors)
- **DynamoDB** — ConsumedRead/WriteCapacityUnits, ThrottledRequests, ItemCount (via describe_table)
- **Lambda** — Invocations, Errors, Throttles, Duration, ConcurrentExecutions
- **API Gateway** — Count, 4XXError, 5XXError, Latency, IntegrationLatency (v1 REST + v2 HTTP)
- **Step Functions** — ExecutionsStarted/Succeeded/Failed/Aborted/TimedOut, ExecutionTime

**Metric naming:** `aws.<service>.<MetricName>.<Stat>` (e.g., `aws.lambda.Invocations.Sum`)

**Documentation:** [docs/forward-metrics.md](./docs/forward-metrics.md)

---

### 5. Supabase Postgres

Connects directly to Supabase Postgres, queries `pg_stat_*` views, and forwards database metrics to New Relic.

| Component | File | Description |
|-----------|------|-------------|
| Entry point | `supabase/main.py` | Orchestrates collection and shipping, handles config and error handling |
| Collector | `supabase/collectors/supabase_collector.py` | Queries pg_stat_* views (db_size, connections, table_stats, bgwriter, statements) |
| Shipper | `supabase/shippers/newrelic_shipper.py` | Batches metrics to New Relic Metric API (2,000/batch), ships run log to Log API, with retry logic |
| Workflow | `.github/workflows/forward-supabase.yml` | Runs every hour |

**Metrics collected:**
- Database size (`supabase.db.size_bytes`)
- Connection states (`supabase.connections.total`, `supabase.connections.by_state`)
- Table stats — live/dead rows, inserts/updates/deletes, seq/idx scans, table size (`supabase.table.*`)
- Background writer stats (`supabase.bgwriter.*`)
- Top-20 slowest queries by mean execution time (`supabase.statements.*`)

**Metric namespace:** `supabase.*`

**Documentation:** [docs/supabase-main.md](./docs/supabase-main.md) | [docs/supabase_collector.md](./docs/supabase_collector.md) | [docs/newrelic_shipper.md](./docs/newrelic_shipper.md)

---

## File Structure

```
grafana setup/
├── README.md                          # This file — project overview
├── Setup · MD.md                      # Detailed setup guide (CloudWatch logs & metrics)
├── WEB_TRAFFIC_README.md              # CloudFront web traffic documentation
├── requirements.txt                   # Python deps: boto3, botocore
│
├── docs/                              # Individual processor documentation
│   ├── webtraffic_processor.md
│   ├── forward-web-traffic.md
│   ├── cur_processor.md
│   ├── forward_cur.md
│   ├── forwarder.md
│   ├── forward-metrics.md
│   ├── supabase-main.md
│   ├── supabase_collector.md
│   └── newrelic_shipper.md
│
├── webtraffic_processor.py            # CloudFront log processor
├── forward-web-traffic.py             # Web traffic → New Relic forwarder
├── cur_processor.py                   # AWS CUR processor
├── forward_cur.py                     # CUR → New Relic forwarder
├── forwarder.py                       # CloudWatch Logs → New Relic forwarder
├── forward-metrics.py                 # CloudWatch Metrics → New Relic forwarder
├── dashboard.json                     # New Relic dashboard (22 widgets)
│
├── supabase/                          # Supabase monitoring subpackage
│   ├── main.py                        # Entry point
│   ├── requirements.txt               # psycopg2, requests, urllib3
│   ├── collectors/
│   │   └── supabase_collector.py      # Postgres metrics collector
│   └── shippers/
│       └── newrelic_shipper.py        # New Relic API shipper
│
├── state.json                         # CloudWatch Logs state (last-fetch timestamps)
├── metrics-state.json                 # CloudWatch Metrics state
│
└── .github/workflows/                 # GitHub Actions workflows
    ├── forward-web-traffic.yml        # Every 4 hours
    ├── forward-cur.yml                # Every 8 hours
    ├── forward-logs.yml               # Every hour
    ├── forward-metrics.yml            # Daily @ 7 PM
    └── forward-supabase.yml           # Every hour
```

---

## Schedule Overview

| Workflow | Schedule | Frequency | Purpose |
|----------|----------|-----------|---------|
| `forward-web-traffic.yml` | `0 */4 * * *` | Every 4 hours | Process CloudFront logs & forward metrics |
| `forward-cur.yml` | `0 */8 * * *` | Every 8 hours | Process AWS CUR data & forward cost metrics |
| `forward-supabase.yml` | `0 * * * *` | Every hour | Collect Supabase Postgres metrics |
| `forward-logs.yml` | `0 * * * *` | Every hour | Forward CloudWatch Logs to New Relic |
| `forward-metrics.yml` | `0 19 * * *` | Daily @ 7 PM | Forward CloudWatch Metrics to New Relic |

---

## Cost Analysis

### New Relic (Primary Cost Driver)

| Tier | Monthly Ingest | Cost | Notes |
|------|---------------|------|-------|
| **Free** | 100 GB | **$0** | 1 platform user, unlimited basic users, full dashboards & alerting |
| Pro | 500 GB+ | ~$0.25/GB over 100 GB | Not needed for this stack |

**Estimated monthly ingest:**

| Pipeline | Frequency | Data/run | Data/day | Data/month |
|----------|-----------|----------|----------|------------|
| Web Traffic | Every 4h (6 runs/day) | ~5-10 MB | ~30-60 MB | ~900 MB - 1.8 GB |
| CloudWatch Logs | Every hour (24 runs/day) | ~1-5 MB* | ~24-120 MB | ~720 MB - 3.6 GB |
| CloudWatch Metrics | Daily (1 run/day) | ~1-2 MB | ~1-2 MB | ~30-60 MB |
| CUR | Every 8h (3 runs/day) | ~1-2 MB | ~3-6 MB | ~90-180 MB |
| Supabase | Every hour (24 runs/day) | ~0.1-0.5 MB | ~2.4-12 MB | ~72-360 MB |
| **Total** | | | **~60-190 MB/day** | **~1.8-5.7 GB/month** |

> *CloudWatch Logs ingest varies widely based on log volume. For low-traffic environments, expect <5 MB/run. The `LOG_GROUP_PREFIX` env var can filter noisy log groups if approaching the 100 GB limit.

**Conclusion:** Even at the high end (~5.7 GB/month), this is **~4.3% of the 100 GB Free tier**. No paid New Relic tier is required.

### AWS Costs

| Resource | Monthly Cost | Notes |
|----------|-------------|-------|
| **S3** (CloudFront logs) | ~$0.01-0.05/GB | Minimal — logs are small |
| **CloudWatch Logs** | ~$0.50/GB ingested | Only if log volume is high |
| **CloudWatch Metrics** | ~$0.30/metric/month | ~100 metrics = ~$30/month |
| **S3 Storage** (CUR data) | ~$0.023/GB | Minimal — CUR files are small |
| **OIDC** | $0 | No long-lived credentials |
| **Total** | **~$0-35/month** | Typically <$10/month for small environments |

### GitHub Actions Costs

| Plan | Minutes/month | Cost |
|------|--------------|------|
| **Free** | 2,000 min (public repos: unlimited) | **$0** |
| Pro | 3,000 min | $0.008/min over limit |

**Estimated monthly usage:**

| Workflow | Runs/day | Duration | Minutes/day | Minutes/month |
|----------|----------|----------|-------------|---------------|
| Web Traffic | 6 | ~2 min | ~12 | ~360 |
| CloudWatch Logs | 24 | ~1 min | ~24 | ~720 |
| CloudWatch Metrics | 1 | ~3 min | ~3 | ~90 |
| CUR | 3 | ~2 min | ~6 | ~180 |
| Supabase | 24 | ~1 min | ~24 | ~720 |
| **Total** | | | **~71 min/day** | **~2,070 min/month** |

**Conclusion:** For public repositories, GitHub Actions is completely free (unlimited minutes). For private repos on the Free plan, this slightly exceeds 2,000 minutes — consider upgrading to Pro ($4/user/month) or reducing frequency.

### Summary

| Component | Monthly Cost |
|-----------|-------------|
| New Relic (Free tier) | **$0** |
| AWS (S3, CloudWatch, etc.) | **$0-35** (typically <$10) |
| GitHub Actions (public repo) | **$0** |
| GitHub Actions (private repo, Free plan) | **$0** (upgrade to Pro if >2,000 min) |
| **Total** | **$0-35/month** |

---

## Setup Guide

### Prerequisites

| Tool | Version |
|------|---------|
| AWS CLI | 2.x |
| Python | 3.12 |
| GitHub Actions runner | ubuntu-latest |

### Quick Start

1. **Sign up for New Relic** — https://newrelic.com/signup (no credit card required)
2. **Get your ingest license key** — New Relic → API Keys → INGEST - LICENSE (starts with `NRAK-`)
3. **Configure AWS OIDC** — See [Setup Guide](./Setup%20%C2%B7%20MD.md) for detailed instructions
4. **Add GitHub secrets:**
   - `AWS_DEPLOY_ROLE_ARN` — ARN of IAM role for OIDC
   - `NEW_RELIC_LICENSE_KEY` — New Relic ingest license key
5. **For web traffic:** Also add `CLOUDFRONT_LOGS_BUCKET` and configure CloudFront logging to S3
6. **For Supabase:** Also add `SUPABASE_DB_URL` (Postgres connection string)
7. **Push to main** — workflows will start running on their schedules

### GitHub Secrets Reference

| Secret | Used By | Description |
|--------|---------|-------------|
| `AWS_DEPLOY_ROLE_ARN` | All AWS workflows | ARN of IAM role for OIDC authentication |
| `NEW_RELIC_LICENSE_KEY` | All workflows | New Relic ingest license key (starts with `NRAK-`) |
| `CLOUDFRONT_LOGS_BUCKET` | Web Traffic | S3 bucket name for CloudFront logs |
| `SUPABASE_DB_URL` | Supabase | Postgres connection string |
| `RAW_CUR_BUCKET` | CUR | S3 bucket containing CUR data (default: `stellarglobal-costing-bucket`) |

### GitHub Variables (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_REGION` | `us-east-1` | AWS region for all API calls |
| `NEW_RELIC_LOGS_URL` | Auto | Override New Relic Log API endpoint (EU: `https://log-api.eu.newrelic.com/log/v1`) |
| `NEW_RELIC_METRICS_URL` | Auto | Override New Relic Metric API endpoint (EU: `https://metric-api.eu.newrelic.com/metric/v1`) |
| `LOG_GROUP_PREFIX` | (empty) | Filter CloudWatch log groups by prefix |
| `BATCH_SIZE` | `500` | Batch size for New Relic API calls |
| `LOOKBACK_HOURS` | `5` | Lookback window for first-run log fetching |

---

## Security

- **No long-lived credentials:** All AWS access uses OIDC federation — no IAM keys stored in GitHub
- **Secrets in GitHub:** New Relic license key and AWS role ARN stored only in GitHub secrets
- **Read-only AWS access:** IAM role has read-only permissions for CloudWatch, S3, Lambda, etc.
- **No sensitive data in state:** `state.json` and `metrics-state.json` contain only resource names and timestamps
- **Supabase uses direct Postgres:** No REST API keys needed — direct read-only Postgres connection only

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| No data in New Relic | Wrong data centre (US vs EU) | Set `NEW_RELIC_LOGS_URL` / `NEW_RELIC_METRICS_URL` to EU endpoints |
| 403 from New Relic API | Expired/wrong license key | Re-generate key in New Relic → API Keys |
| Duplicate log events | Concurrent workflow runs | Ensure `concurrency` block is present in workflow |
| S3 metrics show 0 | Storage Lens not enabled | Forwarder uses direct ListObjectsV2 as fallback |
| DynamoDB ItemCount shows 0 | On-demand table | Forwarder uses `describe_table()` as fallback |
| State file conflicts | Concurrent commits to `state` branch | Workflows use `concurrency` + fresh clone pattern |

---

## Related Documentation

- [Setup Guide](./Setup%20%C2%B7%20MD.md) — Detailed AWS OIDC setup, IAM permissions, New Relic configuration
- [Web Traffic README](./WEB_TRAFFIC_README.md) — CloudFront web traffic monitoring documentation
- [Processor Docs](./docs/) — Individual documentation for each processor and forwarder
- [New Relic Metric API](https://docs.newrelic.com/docs/apis/nerdgraph/examples/nerdgraph-metric-api/)
- [CloudFront Access Logs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html)
- [GitHub Actions OIDC](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services)
