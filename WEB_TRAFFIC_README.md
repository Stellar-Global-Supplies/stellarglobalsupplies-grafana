# CloudFront Web Traffic Monitoring

Comprehensive web traffic analytics pipeline that processes CloudFront access logs and forwards metrics to New Relic for visualization.

## Architecture

```
CloudFront Access Logs (S3)
         │
         ├── webtraffic_processor.py
         │       ├── Downloads .gz log files from S3
         │       ├── Parses CloudFront JSON logs
         │       ├── Extracts 15+ metrics (status, browser, OS, cache, etc.)
         │       └── Generates web-traffic-report.json (7-day rolling window)
         │
         ├── forward-web-traffic.py
         │       ├── Reads web-traffic-report.json
         │       ├── Converts to 50+ New Relic metrics
         │       └── Pushes to New Relic Metric API (aws.webtraffic.*)
         │
         └── GitHub Actions (every 8 hours)
                 ├── Processes logs
                 ├── Forwards metrics
                 └── Commits report to state branch
```

## Files

| File | Purpose |
|------|---------|
| `webtraffic_processor.py` | Processes CloudFront logs from S3, generates JSON report |
| `forward-web-traffic.py` | Reads report and forwards metrics to New Relic |
| `.github/workflows/forward-web-traffic.yml` | GitHub Actions workflow (runs every 8 hours) |
| `dashboard.json` | New Relic dashboard configuration (22 widgets) |

## Metrics Collected

### Traffic Metrics
- **Total Requests** - Total number of requests in 7-day window
- **Unique IPs** - Number of unique visitor IP addresses
- **Avg Daily** - Average daily request volume
- **Peak Hour** - Hour with highest traffic (UTC)

### Performance Metrics
- **Error Rate %** - Percentage of 4xx + 5xx responses
- **Cache Hit %** - CloudFront cache hit ratio
- **Avg Response Size** - Average response payload size (bytes)
- **Avg Response Time** - Average time-taken from logs (seconds)
- **HTTPS Usage %** - Percentage of HTTPS/HTTP2/HTTP3 requests

### HTTP Status Distribution
- **2xx** - Successful responses
- **3xx** - Redirects
- **4xx** - Client errors
- **5xx** - Server errors

### User Agent Analysis
- **Browser Distribution** - Chrome, Safari, Firefox, Edge, Opera, IE
- **OS Distribution** - Windows, macOS, Linux, Android, iOS, Chrome OS

### Behavior Metrics
- **HTTP Methods** - GET, POST, PUT, DELETE, etc.
- **Top Pages** - Most visited URIs (top 10)
- **Top Referrers** - Traffic sources (top 10)

### Geographic Distribution
- **Countries** - Top 8 countries by request volume
- **Edge Locations** - Mapped to countries (India, UAE, USA, Europe, etc.)

### Device Split
- **Mobile** - Smartphones and tablets
- **Desktop** - Traditional computers
- **Tablet** - Tablet devices

### Time Patterns
- **Traffic Over Time** - Daily request volume (7-day trend)
- **Hourly Pattern** - Request distribution by hour (0-23)

## Dashboard Widgets (22 total)

### Row 1 - Key Metrics (Billboards)
1. Total Requests (7-day)
2. Unique IPs (7-day)
3. Avg Daily Requests
4. Peak Hour

### Row 2 - Time & Device
5. Traffic Over Time (7-day) - Line chart
6. Device Split - Pie chart

### Row 3 - Top Content & Geography
7. Top 10 Pages by Visits - Bar chart
8. Geo Distribution (Top Countries) - Bar chart

### Row 4 - Time Patterns
9. Hourly Traffic Pattern - Area chart

### Row 5 - Device Breakdown
10. Mobile vs Desktop % - Bar chart
11. Web Traffic Summary - Table

### Row 6 - HTTP & Performance
12. HTTP Status Distribution - Bar chart
13. Error Rate % - Billboard
14. Cache Hit Ratio % - Billboard

### Row 7 - User Agent
15. Browser Distribution - Pie chart
16. OS Distribution - Pie chart

### Row 8 - Behavior & Security
17. HTTP Methods - Bar chart
18. Top 10 Referrers - Table
19. Avg Response Size (bytes) - Billboard
20. Avg Response Time (s) - Billboard
21. HTTPS Usage % - Billboard

## Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.12+ |
| AWS CLI | 2.x |
| boto3 | 1.34.0+ |
| New Relic Account | Free tier or higher |

## Setup

### 1. AWS S3 Bucket

CloudFront must be configured to write access logs to S3:

```bash
# CloudFront distribution settings:
# - Enable logging: Yes
# - Bucket: stellarglobal-cf-logs
# - Prefix: AWSLogs/
# - Log format: JSON (recommended)
```

### 2. GitHub Secrets

Add these secrets in your repository settings:

| Secret | Value |
|--------|-------|
| `NEW_RELIC_LICENSE_KEY` | New Relic ingest license key (starts with `NRAK-`) |
| `AWS_DEPLOY_ROLE_ARN` | ARN of IAM role for OIDC authentication |
| `CLOUDFRONT_LOGS_BUCKET` | S3 bucket name (default: `stellarglobal-cf-logs`) |

### 3. AWS IAM Permissions

The GitHub Actions OIDC role needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::stellarglobal-cf-logs",
        "arn:aws:s3:::stellarglobal-cf-logs/*"
      ]
    }
  ]
}
```

### 4. New Relic Dashboard

The dashboard is automatically configured in `dashboard.json`. To import:

1. Go to New Relic → Dashboards
2. Click "Import dashboard"
3. Paste the contents of `dashboard.json`
4. Update the `accountIds` field with your New Relic account ID

## Schedule

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| `forward-web-traffic.yml` | Every 8 hours (`0 */8 * * *`) | Process logs & forward metrics |
| `forward-cur.yml` | Every 8 hours (`0 */8 * * *`) | AWS cost data |
| `forward-supabase.yml` | Every hour (`0 * * * *`) | Supabase metrics |
| `forward-logs.yml` | Every hour (`0 * * * *`) | CloudWatch logs |
| `forward-metrics.yml` | Daily at 7 PM (`0 19 * * *`) | CloudWatch metrics |

## CloudFront Log Fields Parsed

The processor extracts these fields from CloudFront JSON logs:

| Field | Description | Example |
|-------|-------------|---------|
| `timestamp` | Request timestamp | `2026-03-28T04:12:00Z` |
| `c-ip` | Client IP address | `203.0.113.42` |
| `cs-uri-stem` | Request URI | `/products/widget` |
| `cs-user-agent` | Browser user agent | `Mozilla/5.0 (Windows NT 10.0; Win64; x64)` |
| `sc-status` | HTTP status code | `200`, `404`, `500` |
| `sc-bytes` | Response size (bytes) | `12345` |
| `cs-bytes` | Request size (bytes) | `456` |
| `cs-method` | HTTP method | `GET`, `POST` |
| `cs-referer` | Referrer URL | `https://google.com` |
| `x-edge-result-type` | Cache status | `Hit`, `Miss` |
| `time-taken` | Response time (seconds) | `0.045` |
| `cs-protocol` | Protocol | `HTTPS`, `HTTP/2` |
| `x-edge-location` | Edge location code | `BOM`, `JFK`, `LHR` |

## Metric Namespace

All web traffic metrics use the namespace: `aws.webtraffic.*`

Examples:
- `aws.webtraffic.summary.total_requests`
- `aws.webtraffic.traffic.daily.requests`
- `aws.webtraffic.pages.visits`
- `aws.webtraffic.http.status.count`
- `aws.webtraffic.browsers.pct`

## Local Testing

### Process logs locally:

```bash
# Set environment variables
export AWS_REGION=us-east-1
export LOGS_BUCKET=stellarglobal-cf-logs

# Run processor
python webtraffic_processor.py

# Output: web-traffic-report.json
```

### Forward metrics locally:

```bash
# Set environment variables
export NEW_RELIC_LICENSE_KEY=your-key-here
export NEW_RELIC_REGION=eu
export WEB_TRAFFIC_REPORT=web-traffic-report.json

# Run forwarder
python forward-web-traffic.py

# Output: Metrics pushed to New Relic
```

### View the report:

```bash
cat web-traffic-report.json | jq .
```

## Troubleshooting

### No logs in S3

**Problem**: Processor reports "No data — skipping report generation"

**Solutions**:
- Verify CloudFront logging is enabled
- Check S3 bucket name is correct
- Ensure logs are in `AWSLogs/` prefix
- Verify IAM permissions for S3 access

### Empty bucket name error

**Problem**: `ParamValidationError: Invalid bucket name ""`

**Solution**: Bucket name is hardcoded to `stellarglobal-cf-logs`. Update the `BUCKET` variable in `webtraffic_processor.py` if your bucket name is different.

### Metrics not appearing in New Relic

**Problem**: Dashboard shows no data

**Solutions**:
- Verify `NEW_RELIC_LICENSE_KEY` is correct
- Check workflow run logs for errors
- Ensure metrics are being pushed (check workflow output)
- Verify account ID in dashboard queries matches your New Relic account
- Wait 5-10 minutes for data to appear (New Relic ingestion delay)

### Time-series chart shows flat line

**Problem**: Traffic Over Time chart shows no variation

**Solutions**:
- Verify timestamps are being set correctly in forwarder
- Check that `traffic.daily.requests` metrics have different timestamps
- Ensure SINCE clause is `7 days ago` or longer

## NRQL Queries

### Example queries for New Relic:

```sql
-- Total requests last 7 days
SELECT sum(`aws.webtraffic.traffic.daily.requests`) 
FROM Metric 
WHERE source = 'cloudfront-logs' 
SINCE 7 days ago

-- Top pages
SELECT latest(`aws.webtraffic.pages.visits`) AS 'Visits' 
FROM Metric 
WHERE source = 'cloudfront-logs' 
FACET page 
SINCE 1 day ago 
LIMIT 10

-- Error rate trend
SELECT latest(`aws.webtraffic.summary.error_rate`) AS 'Error Rate %' 
FROM Metric 
WHERE source = 'cloudfront-logs' 
SINCE 7 days ago 
TIMESERIES 1 day

-- Browser breakdown
SELECT latest(`aws.webtraffic.browsers.pct`) 
FROM Metric 
WHERE source = 'cloudfront-logs' 
FACET browser 
SINCE 1 day ago

-- Geographic distribution
SELECT latest(`aws.webtraffic.geo.requests`) AS 'Requests' 
FROM Metric 
WHERE source = 'cloudfront-logs' 
FACET country 
SINCE 1 day ago 
LIMIT 10
```

## Cost

**$0/month** - Uses New Relic Free Edition:
- 100 GB/month ingest (logs + metrics combined)
- No credit card required
- No expiry

Typical usage: ~5-10 MB per workflow run (every 8 hours) = ~15-30 MB/day = ~450-900 MB/month

## Security

- **No long-lived credentials**: Uses AWS OIDC for authentication
- **Secrets in GitHub**: New Relic license key stored in GitHub secrets
- **No sensitive data in state**: State files contain only resource names and timestamps
- **Read-only S3 access**: Processor only reads logs, never writes to S3

## Maintenance

### Reset state (force re-process all logs):

```bash
# Delete state branch file
git checkout state
git rm web-traffic-report.json
git commit -m "chore: reset web traffic state [skip ci]"
git push origin state
```

### Update schedule:

Edit `.github/workflows/forward-web-traffic.yml`:

```yaml
on:
  schedule:
    - cron: "0 */8 * * *"  # Change this
```

### Manual trigger:

GitHub → Actions → CloudFront Web Traffic → New Relic → Run workflow

## Related Documentation

- [Setup Guide](./Setup%20%C2%B7%20MD.md) - Main setup documentation
- [New Relic Metric API](https://docs.newrelic.com/docs/apis/nerdgraph/examples/nerdgraph-metric-api/)
- [CloudFront Access Logs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html)
- [GitHub Actions OIDC](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services)

## Support

For issues or questions:
1. Check workflow run logs in GitHub Actions
2. Review New Relic ingestion errors
3. Verify AWS CloudWatch logs for Lambda errors
4. Check `web-traffic-report.json` in the state branch