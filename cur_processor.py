"""
CUR Processor - Downloads and processes AWS CUR data from S3

Reads CUR manifest from S3, downloads and decompresses CSV data,
transforms records, and generates 4 JSON output files:
  - costs.json          : granular daily cost per service per region
  - daily-costs.json    : daily total + per-service rollup
  - summary.json        : monthly total per service
  - costs-by-tag.json   : cost breakdown by application tag + uncategorized

Also supports processing a local CSV file directly (for testing/backfill):
  python cur_processor.py --local <path-to-csv>

Environment variables:
  AWS_REGION              - AWS region (default: us-east-1)
  RAW_CUR_BUCKET          - S3 bucket containing CUR data (default: stellarglobal-costing-bucket)
"""

import json
import os
import sys
import gzip
import csv
import io
import re
from datetime import datetime
from typing import Any
import logging

# ── Logging setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

# ── Configuration ─────────────────────────────────────────────────────────────
REGION     = os.environ.get('AWS_REGION', 'us-east-1')
RAW_BUCKET = os.environ.get('RAW_CUR_BUCKET', 'stellarglobal-costing-bucket')

# ── Service name mapping (ProductCode → human-readable) ───────────────────────
SERVICE_NAME_MAP = {
    'AWSCloudFormation':  'AWS CloudFormation',
    'AWSDataTransfer':    'AWS Data Transfer',
    'AWSGlue':            'AWS Glue',
    'AWSLambda':          'AWS Lambda',
    'AWSQueueService':    'Amazon SQS',
    'AWSSecretsManager':  'AWS Secrets Manager',
    'AWSXRay':            'AWS X-Ray',
    'AmazonApiGateway':   'Amazon API Gateway',
    'AmazonBedrock':      'Amazon Bedrock',
    'AmazonCloudFront':   'Amazon CloudFront',
    'AmazonCloudWatch':   'Amazon CloudWatch',
    'AmazonDynamoDB':     'Amazon DynamoDB',
    'AmazonRoute53':      'Amazon Route 53',
    'AmazonS3':           'Amazon S3',
    'AmazonSNS':          'Amazon SNS',
    'AmazonStates':       'AWS Step Functions',
    'awskms':             'AWS KMS',
}

# ── Resource grouping: map usageType patterns → logical resource group ─────────
# These let the dashboard group things like "NovaLite" vs "NovaPro" tokens,
# or all S3 request types under one label.
USAGE_GROUP_PATTERNS = [
    # Bedrock models
    (re.compile(r'NovaLite',  re.I), 'bedrock-nova-lite'),
    (re.compile(r'NovaPro',   re.I), 'bedrock-nova-pro'),
    (re.compile(r'Claude',    re.I), 'bedrock-claude'),
    (re.compile(r'Titan',     re.I), 'bedrock-titan'),
    # Lambda
    (re.compile(r'Lambda-GB-Second|Lambda-GB-Sec', re.I), 'lambda-compute'),
    (re.compile(r'Request',   re.I), 'lambda-requests'),
    # S3
    (re.compile(r'TimedStorage', re.I),     's3-storage'),
    (re.compile(r'Requests-Tier1', re.I),   's3-put-requests'),
    (re.compile(r'Requests-Tier2', re.I),   's3-get-requests'),
    (re.compile(r'DataTransfer',  re.I),    'data-transfer'),
    (re.compile(r'CloudFront-Out', re.I),   'cloudfront-transfer'),
    # CloudWatch
    (re.compile(r'GMD-Metrics|GetMetricData', re.I), 'cloudwatch-metrics-query'),
    (re.compile(r'MetricMonitorUsage',        re.I), 'cloudwatch-alarms'),
    (re.compile(r'VendedLog',                 re.I), 'cloudwatch-logs-ingestion'),
    (re.compile(r'DataScanned',               re.I), 'cloudwatch-logs-insights'),
    # Route53
    (re.compile(r'HostedZone',  re.I), 'route53-hosted-zones'),
    (re.compile(r'DNS-Queries', re.I), 'route53-dns-queries'),
    # Secrets Manager
    (re.compile(r'AWSSecretsManager-Secrets', re.I), 'secrets-storage'),
    (re.compile(r'AWSSecretsManagerAPIRequest|AWSSecretsManager-API', re.I), 'secrets-api-calls'),
    # API Gateway
    (re.compile(r'ApiGatewayHttpRequest', re.I), 'apigw-http-requests'),
    # DynamoDB
    (re.compile(r'WriteRequestUnits|WriteCapacity', re.I), 'dynamodb-write'),
    (re.compile(r'ReadRequestUnits|ReadCapacity',   re.I), 'dynamodb-read'),
    (re.compile(r'TimedStorage',                    re.I), 'dynamodb-storage'),
    # KMS
    (re.compile(r'KMS-Requests', re.I), 'kms-requests'),
    # Glue
    (re.compile(r'Catalog-Request', re.I), 'glue-catalog-requests'),
    (re.compile(r'Catalog-Storage', re.I), 'glue-catalog-storage'),
    # Step Functions
    (re.compile(r'StateTransition', re.I), 'stepfunctions-transitions'),
    # X-Ray
    (re.compile(r'XRay-TracesStored', re.I), 'xray-traces'),
    # CloudFront
    (re.compile(r'Requests-Tier1',  re.I), 'cf-requests'),
    (re.compile(r'Requests-Tier2',  re.I), 'cf-https-requests'),
    (re.compile(r'DataTransfer-Out', re.I), 'cf-data-transfer'),
    (re.compile(r'CloudFrontFunctions', re.I), 'cf-functions'),
    (re.compile(r'Invalidations',   re.I), 'cf-invalidations'),
]

# ── Application Tag Normalization ─────────────────────────────────────────────
# Maps any tag variant → canonical app group name used in dashboard
APP_TAG_GROUPS = {
    'oms':                 'oms-app',
    'oms-app':             'oms-app',
    'oms_app':             'oms-app',
    'order-management':    'oms-app',
    'cleanup':             'cleanup-automation',
    'cleanup-automation':  'cleanup-automation',
    'cleanup_automation':  'cleanup-automation',
    'observe':             'observe-app',
    'observe-app':         'observe-app',
    'observer':            'observe-app',
    'workflow':            'workflow-platform',
    'workflow-platform':   'workflow-platform',
    'wf':                  'workflow-platform',
    'wf-platform':         'workflow-platform',
    'ops':                 'ops-platform',
    'ops-platform':        'ops-platform',
    'ops_platform':        'ops-platform',
    'global':              'ops-platform',
    'stellar-ops':         'ops-platform',
    'quote':               'quote-app',
    'quote-app':           'quote-app',
    'quotation':           'quote-app',
}

def normalize_app_tag(tag: str) -> str | None:
    """
    Normalize application tag to canonical short name.
    Checks for substring matches so partial tags still resolve.
    Returns None if empty / unrecognised.
    """
    if not tag or not tag.strip():
        return None
    lower = tag.lower().strip()

    # Exact or direct lookup first
    if lower in APP_TAG_GROUPS:
        return APP_TAG_GROUPS[lower]

    # Substring scan (longest match wins for specificity)
    best_key  = None
    best_len  = 0
    for key, canonical in APP_TAG_GROUPS.items():
        if key in lower and len(key) > best_len:
            best_key = canonical
            best_len = len(key)
    if best_key:
        return best_key

    # Fall back: sanitise the raw tag so it's safe as a label
    return re.sub(r'[^a-z0-9_-]', '-', lower).strip('-') or None


# ── Resource-ID based app inference ───────────────────────────────────────────
# AWS CUR lineItem/ResourceId contains the ARN or bare name of the resource.
# When cost-allocation tags are absent (no resourceTags/* columns), we parse
# the resource ID using the project's own naming conventions observed across
# Lambda functions, DynamoDB tables, S3 buckets, and API Gateway names.
#
# Rules are evaluated in order; the FIRST match wins.
# Each rule is (compiled_regex, canonical_app_group).
# The regex is matched against the lower-cased resource name/ARN segment.
#
# Naming conventions seen in state.json / repo:
#   stellar-oms-*                → oms-app
#   sgs-quote-app-* / stellar-quote-* → quote-app
#   stellar-wf-prod-* / stellar-wf-*  → workflow-platform
#   stellar-observe-prod-* / stellar-observe-* → observe-app
#   stellar-cleanup-prod-*       → cleanup-automation
#   stellar-global-prod-* / stellarglobal-ops-* / stellar-global-* → ops-platform
#   meta-analytics-* / stellar-daily-processor / stellar-report → ops-platform
#   stellar-auth / stellar-seed-user → ops-platform
#   stellar-global-costing-bucket / cur / billing → ops-platform (infra)
RESOURCE_ID_PATTERNS: list[tuple[re.Pattern, str]] = [
    # OMS — order management service
    (re.compile(r'stellar[-_]oms',              re.I), 'oms-app'),
    (re.compile(r'order[-_]management',         re.I), 'oms-app'),

    # Quote app
    (re.compile(r'sgs[-_]quote',                re.I), 'quote-app'),
    (re.compile(r'stellar[-_]quote',            re.I), 'quote-app'),

    # Workflow platform (must come before generic 'stellar-wf' catch-all)
    (re.compile(r'stellar[-_]wf[-_]prod',       re.I), 'workflow-platform'),
    (re.compile(r'stellar[-_]wf\b',             re.I), 'workflow-platform'),
    (re.compile(r'workflow[-_]platform',        re.I), 'workflow-platform'),

    # Observe app
    (re.compile(r'stellar[-_]observe[-_]prod',  re.I), 'observe-app'),
    (re.compile(r'stellar[-_]observe\b',        re.I), 'observe-app'),

    # Cleanup automation
    (re.compile(r'stellar[-_]cleanup[-_]prod',  re.I), 'cleanup-automation'),
    (re.compile(r'stellar[-_]cleanup\b',        re.I), 'cleanup-automation'),

    # Ops / infra platform — catch-all for global/infra resources
    (re.compile(r'stellar[-_]global[-_]prod',   re.I), 'ops-platform'),
    (re.compile(r'stellarglobal[-_]ops',        re.I), 'ops-platform'),
    (re.compile(r'stellar[-_]global\b',         re.I), 'ops-platform'),
    (re.compile(r'stellarglobal',               re.I), 'ops-platform'),
    (re.compile(r'meta[-_]analytics',           re.I), 'ops-platform'),
    (re.compile(r'stellar[-_]daily[-_]processor',re.I),'ops-platform'),
    (re.compile(r'stellar[-_]report\b',         re.I), 'ops-platform'),
    (re.compile(r'stellar[-_]auth\b',           re.I), 'ops-platform'),
    (re.compile(r'stellar[-_]seed',             re.I), 'ops-platform'),
    (re.compile(r'stellarglobal[-_]costing',    re.I), 'ops-platform'),
    (re.compile(r'awscost|cur[-_]processor',    re.I), 'ops-platform'),
]


def infer_app_from_resource_id(resource_id: str) -> str | None:
    """
    Infer the canonical application group from a CUR lineItem/ResourceId.

    The ResourceId can be:
      - A full ARN:  arn:aws:lambda:us-east-1:123456789:function:stellar-oms-create-order
      - A bare name: stellar-oms-create-order
      - An S3 ARN:   arn:aws:s3:::stellarglobal-costing-bucket
      - A log group: /aws/lambda/stellar-oms-create-order
      - Empty string or '-' for shared/global services

    We extract the final name segment from ARNs and match against
    RESOURCE_ID_PATTERNS.  Returns None if nothing matches (caller will
    keep the record as 'uncategorized').
    """
    if not resource_id or resource_id.strip() in ('', '-'):
        return None

    rid = resource_id.strip()

    # Extract the meaningful name from an ARN (last ':' segment)
    if rid.startswith('arn:'):
        rid = rid.split(':')[-1]

    # For log group paths like /aws/lambda/stellar-oms-create-order
    # take the last path component
    if '/' in rid:
        rid = rid.rstrip('/').split('/')[-1]

    lower = rid.lower()

    for pattern, canonical in RESOURCE_ID_PATTERNS:
        if pattern.search(lower):
            return canonical

    return None


def get_usage_group(product_code: str, usage_type: str) -> str:
    """Return a logical resource group label for a usage-type string."""
    for pattern, group in USAGE_GROUP_PATTERNS:
        if pattern.search(usage_type):
            return group
    # Default: strip region prefix (e.g. "USE1-") and lower-case
    stripped = re.sub(r'^[A-Z0-9]+-', '', usage_type)
    return re.sub(r'[^a-z0-9_-]', '-', stripped.lower()).strip('-') or 'other'


# ── CSV Parser ────────────────────────────────────────────────────────────────
def parse_csv_line(line: str) -> list[str]:
    result = []
    current = []
    in_quotes = False
    i = 0
    while i < len(line):
        char = line[i]
        if char == '"':
            if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                current.append('"')
                i += 2
                continue
            else:
                in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            result.append(''.join(current))
            current = []
            i += 1
            continue
        else:
            current.append(char)
        i += 1
    result.append(''.join(current))
    return result


# ── Record Transformation ─────────────────────────────────────────────────────
def transform_cur_record(headers: list[str], values: list[str]) -> dict[str, Any] | None:
    """Transform a CUR CSV row into a normalised cost record."""
    row: dict[str, str] = {}
    for h, v in zip(headers, values):
        row[h] = v or ''

    # Skip non-Usage rows (Tax, etc.) to avoid double-counting
    line_item_type = row.get('lineItem/LineItemType', '')
    if line_item_type not in ('Usage', 'SavingsPlanCoveredUsage',
                               'DiscountedUsage', 'RIFee', 'SavingsPlanRecurringFee'):
        return None

    def to_float(v: str) -> float:
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    cost = to_float(row.get('lineItem/UnblendedCost', '0'))
    # Only emit records with positive cost (saves space & avoids NR noise)
    if cost == 0.0:
        return None

    product_code = (row.get('lineItem/ProductCode') or
                    row.get('product/servicecode') or 'Unknown')

    usage_type   = row.get('lineItem/UsageType', '')
    usage_group  = get_usage_group(product_code, usage_type)
    service_name = (SERVICE_NAME_MAP.get(product_code)
                    or row.get('product/ProductName')
                    or row.get('product/servicename')
                    or product_code)

    start_date   = (row.get('lineItem/UsageStartDate') or
                    row.get('bill/BillingPeriodStartDate') or '')

    # Resolve region — strip AZ suffix (e.g. "us-east-1a" → "us-east-1")
    raw_region   = (row.get('product/regionCode') or
                    row.get('product/region') or '')
    region       = re.sub(r'[a-z]$', '', raw_region) if raw_region else 'global'

    # ── Application tagging — two-stage resolution ────────────────────────────
    # Stage 1: cost-allocation tags (present only if activated in AWS billing
    #          console; columns like resourceTags/user:Application).
    #          We try every common variant AWS uses in CUR exports.
    raw_tag = ''
    for col in (
        'resourceTags/user:Application',
        'resourceTags/user:application',
        'resourceTags/user:App',
        'resourceTags/user:app',
        'resourceTags/user:Project',
        'resourceTags/user:project',
        'resourceTags/user:Team',
        'resourceTags/user:team',
        'resourceTags/user:Service',
        'resourceTags/user:service',
        'resourceTags/user:Environment',  # sometimes encodes app context
    ):
        val = row.get(col, '')
        if val and val.strip():
            raw_tag = val
            break

    application_tag = normalize_app_tag(raw_tag)

    # Stage 2: if no tag resolved, infer from lineItem/ResourceId.
    # This covers all resources whose cost-allocation tags are either absent
    # from the CUR (not activated in billing console) or simply unset on the
    # individual resource, as long as the resource name follows the project's
    # naming conventions (stellar-oms-*, stellar-wf-prod-*, etc.).
    resource_id = row.get('lineItem/ResourceId', '')
    tag_source  = 'tag'
    if not application_tag:
        application_tag = infer_app_from_resource_id(resource_id)
        if application_tag:
            tag_source = 'resource_id'

    return {
        'timestamp':      start_date,
        'applicationTag': application_tag,
        'tagSource':      tag_source if application_tag else 'none',
        'resourceId':     resource_id,
        'account':        (row.get('lineItem/UsageAccountId') or
                           row.get('bill/PayerAccountId') or ''),
        'service':        product_code,
        'serviceName':    service_name,
        'usageGroup':     usage_group,
        'region':         region,
        'usageType':      usage_type,
        'operation':      row.get('lineItem/Operation', ''),
        'lineItemType':   line_item_type,
        'cost':           cost,
        'blendedCost':    to_float(row.get('lineItem/BlendedCost', '0')),
        'usageAmount':    to_float(row.get('lineItem/UsageAmount', '0')),
    }


# ── Aggregation Functions ─────────────────────────────────────────────────────
def aggregate_daily_costs(records: list[dict]) -> list[dict]:
    """Daily total + per-service breakdown."""
    daily_map: dict[str, dict] = {}
    for record in records:
        date        = record.get('timestamp', '').split('T')[0] or 'unknown'
        service_key = record['service']
        if date not in daily_map:
            daily_map[date] = {}
        if service_key not in daily_map[date]:
            daily_map[date][service_key] = {
                'service':     service_key,
                'serviceName': record['serviceName'],
                'cost':        0.0,
            }
        daily_map[date][service_key]['cost'] += record['cost']

    daily_costs = []
    for date in sorted(daily_map.keys()):
        services  = []
        day_total = 0.0
        for svc in daily_map[date].values():
            services.append({
                'service':     svc['service'],
                'serviceName': svc['serviceName'],
                'cost':        round(svc['cost'], 6),
            })
            day_total += svc['cost']
        daily_costs.append({
            'date':      date,
            'totalCost': round(day_total, 6),
            'services':  sorted(services, key=lambda x: x['cost'], reverse=True),
        })
    return daily_costs


def aggregate_costs_by_tag(records: list[dict]) -> dict[str, Any]:
    """Cost breakdown by application tag + uncategorized."""
    app_map: dict[str, dict] = {}
    for record in records:
        app         = record.get('applicationTag') or 'uncategorized'
        date        = record.get('timestamp', '').split('T')[0] or 'unknown'
        service_key = record['service']
        app_map.setdefault(app, {}).setdefault(date, {}).setdefault(service_key, {
            'service':     service_key,
            'serviceName': record['serviceName'],
            'cost':        0.0,
        })
        app_map[app][date][service_key]['cost'] += record['cost']

    by_application = []
    uncategorized_services: dict[str, dict] = {}
    uncategorized_total = 0.0

    for app, date_map in app_map.items():
        # Flatten services across all dates
        svc_totals: dict[str, dict] = {}
        app_total  = 0.0
        for date_svcs in date_map.values():
            for svc_key, svc in date_svcs.items():
                svc_totals.setdefault(svc_key, {
                    'service':     svc['service'],
                    'serviceName': svc['serviceName'],
                    'cost':        0.0,
                })
                svc_totals[svc_key]['cost'] += svc['cost']
                app_total                   += svc['cost']

        services_list = sorted(
            [{'service': s['service'], 'serviceName': s['serviceName'],
              'cost': round(s['cost'], 6)} for s in svc_totals.values()],
            key=lambda x: x['cost'], reverse=True,
        )

        if app == 'uncategorized':
            uncategorized_services = svc_totals
            uncategorized_total    = app_total
        else:
            by_application.append({
                'application': app,
                'totalCost':   round(app_total, 6),
                'services':    services_list,
            })

    by_application.sort(key=lambda x: x['totalCost'], reverse=True)

    return {
        'byApplication': by_application,
        'uncategorized': {
            'totalCost': round(uncategorized_total, 6),
            'services':  sorted(
                [{'service': s['service'], 'serviceName': s['serviceName'],
                  'cost': round(s['cost'], 6)} for s in uncategorized_services.values()],
                key=lambda x: x['cost'], reverse=True,
            ),
        },
        'generatedAt': datetime.utcnow().isoformat() + 'Z',
    }


def aggregate_by_usage_group(records: list[dict]) -> list[dict]:
    """
    NEW: Granular breakdown by service + usageGroup (resource type).
    Powers sub-service drilldown panels in the dashboard.
    """
    grp_map: dict[str, dict] = {}
    for record in records:
        date  = record.get('timestamp', '').split('T')[0] or 'unknown'
        key   = f"{date}|{record['service']}|{record['usageGroup']}"
        grp_map.setdefault(key, {
            'date':        date,
            'service':     record['service'],
            'serviceName': record['serviceName'],
            'usageGroup':  record['usageGroup'],
            'region':      record.get('region', 'global'),
            'totalCost':   0.0,
            'usageAmount': 0.0,
            'recordCount': 0,
        })
        grp_map[key]['totalCost']   += record['cost']
        grp_map[key]['usageAmount'] += record['usageAmount']
        grp_map[key]['recordCount'] += 1

    result = []
    for v in grp_map.values():
        v['totalCost']   = round(v['totalCost'],   6)
        v['usageAmount'] = round(v['usageAmount'],  6)
        result.append(v)
    result.sort(key=lambda x: (x['date'], -x['totalCost']))
    return result


# ── S3 helpers ────────────────────────────────────────────────────────────────
def get_s3_client():
    import boto3
    return boto3.client('s3', region_name=REGION)


def download_and_decompress(s3_client, bucket: str, key: str) -> str:
    from botocore.exceptions import ClientError
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        if key.endswith('.gz'):
            return gzip.decompress(data).decode('utf-8')
        return data.decode('utf-8')
    except ClientError as e:
        logger.error(f"Failed to download s3://{bucket}/{key}: {e}")
        raise


# ── Core processing ───────────────────────────────────────────────────────────
def process_csv_text(csv_text: str, billing_period: dict) -> None:
    """
    Parse CSV text, transform records, and write all output JSON files.
    billing_period: {'start': '20260701', 'end': '20260801'}
    """
    lines = [line for line in csv_text.split('\n') if line.strip()]
    if len(lines) < 2:
        logger.warning("No data rows in CSV")
        return

    headers = parse_csv_line(lines[0])
    logger.info(f"CSV columns: {len(headers)}, rows: {len(lines) - 1}")

    # Log which tag columns are present
    tag_cols = [h for h in headers if 'tag' in h.lower() or 'resourceTags' in h]
    logger.info(f"Tag columns found: {tag_cols or 'NONE — all costs will be uncategorized'}")

    transformed_records = []
    skipped = 0
    for i in range(1, len(lines)):
        values = parse_csv_line(lines[i])
        if len(values) != len(headers):
            skipped += 1
            continue
        rec = transform_cur_record(headers, values)
        if rec:
            transformed_records.append(rec)

    logger.info(f"Transformed: {len(transformed_records)} cost records, skipped: {skipped}")

    if not transformed_records:
        logger.warning("No cost records to process")
        return

    # Tag stats — break down by resolution method so you can see how many
    # records were resolved via cost-allocation tags vs resource-ID inference
    from collections import Counter
    source_dist = Counter(r.get('tagSource', 'none') for r in transformed_records)
    tagged_total = sum(1 for r in transformed_records if r.get('applicationTag'))
    logger.info(
        f"Tagging: {tagged_total} resolved "
        f"({source_dist.get('tag', 0)} via cost-allocation tags, "
        f"{source_dist.get('resource_id', 0)} via resource-ID inference), "
        f"{source_dist.get('none', 0)} uncategorized"
    )
    tag_dist = Counter(r.get('applicationTag') or 'uncategorized' for r in transformed_records)
    logger.info(f"App distribution: {dict(tag_dist)}")

    start_raw = billing_period.get('start', '')
    end_raw   = billing_period.get('end', '')
    start_8   = re.sub(r'[^0-9]', '', start_raw)[:8]
    end_8     = re.sub(r'[^0-9]', '', end_raw)[:8]
    bp_out    = {'start': start_8, 'end': end_8}
    month_str = f"{start_8[:4]}-{start_8[4:6]}" if len(start_8) >= 6 else 'unknown'

    # ── costs.json ──
    aggregated: dict[str, dict] = {}
    for record in transformed_records:
        date = record.get('timestamp', '').split('T')[0] or 'unknown'
        key  = f"{date}_{record['service']}_{record.get('region', 'global')}"
        aggregated.setdefault(key, {
            'date':             date,
            'service':          record['service'],
            'serviceName':      record['serviceName'],
            'region':           record.get('region', 'global'),
            'totalCost':        0.0,
            'totalBlendedCost': 0.0,
            'totalUsage':       0.0,
            'recordCount':      0,
        })
        aggregated[key]['totalCost']        += record['cost']
        aggregated[key]['totalBlendedCost'] += record['blendedCost']
        aggregated[key]['totalUsage']       += record['usageAmount']
        aggregated[key]['recordCount']      += 1

    aggregated_array = []
    for v in aggregated.values():
        v['totalCost']        = round(v['totalCost'],        6)
        v['totalBlendedCost'] = round(v['totalBlendedCost'], 6)
        v['totalUsage']       = round(v['totalUsage'],       6)
        aggregated_array.append(v)

    with open('costs.json', 'w') as f:
        json.dump(aggregated_array, f, indent=2)
    logger.info(f"Generated costs.json ({len(aggregated_array)} rows)")

    # ── costs-by-usage-group.json (NEW) ──
    usage_group_data = aggregate_by_usage_group(transformed_records)
    with open('costs-by-usage-group.json', 'w') as f:
        json.dump(usage_group_data, f, indent=2)
    logger.info(f"Generated costs-by-usage-group.json ({len(usage_group_data)} rows)")

    # ── daily-costs.json ──
    daily_costs   = aggregate_daily_costs(transformed_records)
    monthly_total = sum(d['totalCost'] for d in daily_costs)
    daily_out = {
        'billingPeriod': bp_out,
        'dailyCosts':    daily_costs,
        'monthlyTotal':  round(monthly_total, 6),
        'generatedAt':   datetime.utcnow().isoformat() + 'Z',
    }
    with open('daily-costs.json', 'w') as f:
        json.dump(daily_out, f, indent=2)
    logger.info("Generated daily-costs.json")

    # ── costs-by-tag.json ──
    costs_by_tag = aggregate_costs_by_tag(transformed_records)
    costs_by_tag['billingPeriod'] = bp_out
    with open('costs-by-tag.json', 'w') as f:
        json.dump(costs_by_tag, f, indent=2)
    logger.info("Generated costs-by-tag.json")

    # ── summary.json ──
    service_monthly: dict[str, dict] = {}
    for record in transformed_records:
        svc = record['service']
        service_monthly.setdefault(svc, {
            'service':     svc,
            'serviceName': record['serviceName'],
            'cost':        0.0,
        })
        service_monthly[svc]['cost'] += record['cost']

    services_rounded = sorted(
        [{'service': s['service'], 'serviceName': s['serviceName'],
          'cost': round(s['cost'], 6)} for s in service_monthly.values()],
        key=lambda x: x['cost'], reverse=True,
    )
    summary = {
        'month':      month_str,
        'totalCost':  round(sum(s['cost'] for s in service_monthly.values()), 6),
        'services':   services_rounded,
    }
    with open('summary.json', 'w') as f:
        json.dump([summary], f, indent=2)
    logger.info("Generated summary.json")

    logger.info(f"CUR processing complete — billing period {start_8}-{end_8}, "
                f"total cost: ${summary['totalCost']:.4f}")


def process_local_csv(csv_path: str) -> None:
    """Process a local CUR CSV file (uncompressed)."""
    logger.info(f"Processing local CUR file: {csv_path}")
    with open(csv_path, newline='', encoding='utf-8') as f:
        csv_text = f.read()

    # Derive billing period from CSV content
    lines = [l for l in csv_text.split('\n') if l.strip()]
    headers = parse_csv_line(lines[0]) if lines else []
    billing_period = {}
    if len(lines) > 1 and headers:
        first_row_values = parse_csv_line(lines[1])
        row = dict(zip(headers, first_row_values))
        billing_period['start'] = row.get('bill/BillingPeriodStartDate', '')
        billing_period['end']   = row.get('bill/BillingPeriodEndDate',   '')

    logger.info(f"Derived billing period from CSV: {billing_period}")
    process_csv_text(csv_text, billing_period)


def process_cur_manifest(manifest: dict[str, Any]) -> None:
    """Process a CUR manifest from S3."""
    logger.info(f"Processing CUR manifest: {manifest.get('reportId')}")
    s3_client = get_s3_client()
    billing_period = manifest.get('billingPeriod', {})

    for report_key in manifest.get('reportKeys', []):
        logger.info(f"Downloading: {report_key}")
        csv_text = download_and_decompress(s3_client, RAW_BUCKET, report_key)
        process_csv_text(csv_text, billing_period)


def process_latest_cur() -> None:
    """Find and process the latest CUR manifest from S3."""
    from botocore.exceptions import ClientError
    logger.info("Starting CUR processing from S3...")
    s3_client = get_s3_client()

    try:
        response = s3_client.list_objects_v2(
            Bucket=RAW_BUCKET, Prefix='awscost/awscost/', MaxKeys=100)
    except ClientError as e:
        logger.error(f"Failed to list S3 objects: {e}")
        return

    if 'Contents' not in response or not response['Contents']:
        logger.info("No files found in S3 bucket")
        return

    manifest_files = sorted(
        [obj for obj in response['Contents']
         if obj.get('Key', '').lower().endswith('manifest.json')],
        key=lambda x: x.get('LastModified', datetime.min),
        reverse=True,
    )

    if not manifest_files:
        logger.info("No manifest files found")
        return

    latest_key = manifest_files[0]['Key']
    logger.info(f"Processing manifest: {latest_key}")

    try:
        manifest_resp = s3_client.get_object(Bucket=RAW_BUCKET, Key=latest_key)
        manifest      = json.loads(manifest_resp['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to download/parse manifest: {e}")
        return

    process_cur_manifest(manifest)
    logger.info("CUR processing completed successfully")


def main():
    args = sys.argv[1:]

    if args and args[0] == '--local':
        # Direct local CSV mode: python cur_processor.py --local <file.csv>
        if len(args) < 2:
            print("Usage: cur_processor.py --local <path/to/awscost.csv>", file=sys.stderr)
            return 1
        process_local_csv(args[1])
    elif args:
        # Manifest JSON path provided
        with open(args[0]) as f:
            manifest = json.load(f)
        process_cur_manifest(manifest)
    else:
        process_latest_cur()

    logger.info("CUR processor finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())