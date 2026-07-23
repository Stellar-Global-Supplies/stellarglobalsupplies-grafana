"""
CUR Processor - Downloads and processes AWS CUR data from S3

Reads CUR manifest from S3, downloads and decompresses CSV data,
transforms records, and generates 4 JSON output files:
  - costs.json          : granular daily cost per service per region
  - daily-costs.json    : daily total + per-service rollup
  - summary.json        : monthly total per service
  - costs-by-tag.json   : cost breakdown by application tag + uncategorized

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
import boto3
from botocore.exceptions import ClientError

# ── Configuration ─────────────────────────────────────────────────────────────
REGION = os.environ.get('AWS_REGION', 'us-east-1')
RAW_BUCKET = os.environ.get('RAW_CUR_BUCKET', 'stellarglobal-costing-bucket')

# ── Logging setup ─────────────────────────────────────────────────────────────
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)


# ── CSV Parser ────────────────────────────────────────────────────────────────
def parse_csv_line(line: str) -> list[str]:
    """
    Parse a single CSV line, handling quoted fields correctly.
    Handles escaped quotes (double quotes within quoted fields).
    """
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


# ── Application Tag Normalization ─────────────────────────────────────────────
def normalize_app_tag(tag: str) -> str | None:
    """
    Normalize application tag value to the short app names used across the dashboard.
    Returns None if tag is empty or None.
    """
    if not tag or not tag.strip():
        return None
    
    lower = tag.lower().strip()
    if 'oms' in lower:
        return 'oms-app'
    if 'cleanup' in lower or 'cleanup_automation' in lower:
        return 'cleanup-automation'
    if 'observe' in lower:
        return 'observe-app'
    if 'workflow' in lower or 'wf' in lower:
        return 'workflow-platform'
    if 'ops' in lower or 'global' in lower or 'ops_platform' in lower:
        return 'ops-platform'
    if 'quote' in lower:
        return 'quote-app'
    
    # If it matches none of the known apps, return the raw tag normalized
    return re.sub(r'[^a-z0-9_-]', '-', lower)


# ── Record Transformation ─────────────────────────────────────────────────────
def transform_cur_record(headers: list[str], values: list[str]) -> dict[str, Any]:
    """Transform a CUR row into a simplified cost record."""
    row = {}
    for h, v in zip(headers, values):
        row[h] = v or ''
    
    def to_float(v: str) -> float:
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0
    
    start_date = row.get('lineItem/UsageStartDate') or row.get('bill/BillingPeriodStartDate') or ''
    
    # Extract Project tag (AWS CUR format: resourceTags/user:Project)
    raw_project_tag = row.get('resourceTags/user:Project', '')
    application_tag = normalize_app_tag(raw_project_tag)
    
    return {
        'timestamp': start_date,
        'applicationTag': application_tag,
        'account': row.get('lineItem/UsageAccountId') or row.get('bill/PayerAccountId') or '',
        'service': row.get('lineItem/ProductCode') or row.get('product/servicecode') or 'Unknown',
        'serviceName': row.get('product/ProductName') or row.get('product/servicename') or row.get('lineItem/ProductCode') or 'Unknown',
        'region': row.get('product/regionCode') or row.get('product/region') or 'us-east-1',
        'usageType': row.get('lineItem/UsageType', ''),
        'operation': row.get('lineItem/Operation', ''),
        'lineItemType': row.get('lineItem/LineItemType', ''),
        'cost': to_float(row.get('lineItem/UnblendedCost', '0')),
        'blendedCost': to_float(row.get('lineItem/BlendedCost', '0')),
        'usageAmount': to_float(row.get('lineItem/UsageAmount', '0')),
    }


# ── Aggregation Functions ─────────────────────────────────────────────────────
def aggregate_daily_costs(records: list[dict]) -> list[dict]:
    """
    Aggregate records into daily cost breakdown by service.
    Returns list of daily cost records.
    """
    # Group by date, then by service
    daily_map = {}
    
    for record in records:
        date = record.get('timestamp', '').split('T')[0] or 'unknown'
        service_key = record['service']
        
        if date not in daily_map:
            daily_map[date] = {}
        
        if service_key not in daily_map[date]:
            daily_map[date][service_key] = {
                'service': service_key,
                'serviceName': record['serviceName'],
                'cost': 0.0,
            }
        
        daily_map[date][service_key]['cost'] += record['cost']
    
    # Convert to sorted array
    daily_costs = []
    for date in sorted(daily_map.keys()):
        services = []
        day_total = 0.0
        for svc in daily_map[date].values():
            services.append({
                'service': svc['service'],
                'serviceName': svc['serviceName'],
                'cost': round(svc['cost'], 6),
            })
            day_total += svc['cost']
        
        daily_costs.append({
            'date': date,
            'totalCost': round(day_total, 6),
            'services': sorted(services, key=lambda x: x['cost'], reverse=True),
        })
    
    return daily_costs


def aggregate_costs_by_tag(records: list[dict]) -> dict[str, Any]:
    """
    Aggregate records into cost breakdown by application tag.
    Returns per-application: total cost, services breakdown, daily costs.
    """
    # Group: application -> date -> service -> cost
    app_map = {}
    
    for record in records:
        app = record.get('applicationTag') or 'uncategorized'
        date = record.get('timestamp', '').split('T')[0] or 'unknown'
        service_key = record['service']
        
        if app not in app_map:
            app_map[app] = {}
        
        if date not in app_map[app]:
            app_map[app][date] = {}
        
        if service_key not in app_map[app][date]:
            app_map[app][date][service_key] = {
                'service': service_key,
                'serviceName': record['serviceName'],
                'cost': 0.0,
            }
        
        app_map[app][date][service_key]['cost'] += record['cost']
    
    by_application = []
    uncategorized_services = {}
    uncategorized_total = 0.0
    
    for app, date_map in app_map.items():
        # Aggregate services across all dates
        service_totals = {}
        daily_costs = []
        
        for date, services in date_map.items():
            day_total = 0.0
            for svc_data in services.values():
                if svc_data['service'] not in service_totals:
                    service_totals[svc_data['service']] = {
                        'service': svc_data['service'],
                        'serviceName': svc_data['serviceName'],
                        'cost': 0.0,
                    }
                service_totals[svc_data['service']]['cost'] += svc_data['cost']
                day_total += svc_data['cost']
            
            daily_costs.append({
                'date': date,
                'cost': round(day_total, 6),
            })
        
        services_list = sorted(service_totals.values(), key=lambda x: x['cost'], reverse=True)
        for svc in services_list:
            svc['cost'] = round(svc['cost'], 6)
        
        total_cost = sum(s['cost'] for s in services_list)
        rounded_total = round(total_cost, 6)
        
        if app == 'uncategorized':
            uncategorized_services = service_totals
            uncategorized_total = rounded_total
        else:
            by_application.append({
                'application': app,
                'totalCost': rounded_total,
                'services': services_list,
                'dailyCosts': sorted(daily_costs, key=lambda x: x['date']),
            })
    
    return {
        'generatedAt': datetime.utcnow().isoformat() + 'Z',
        'billingPeriod': {'start': '', 'end': ''},
        'byApplication': sorted(by_application, key=lambda x: x['totalCost'], reverse=True),
        'uncategorized': {
            'totalCost': round(uncategorized_total, 6),
            'services': sorted(uncategorized_services.values(), key=lambda x: x['cost'], reverse=True) if uncategorized_services else [],
        },
    }


# ── S3 Operations ─────────────────────────────────────────────────────────────
def get_s3_client():
    """Create and return S3 client."""
    return boto3.client('s3', region_name=REGION)


def download_and_decompress(s3_client, bucket: str, key: str) -> str:
    """Download gzipped file from S3 and return decompressed content as string."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        compressed_content = response['Body'].read()
        decompressed_content = gzip.decompress(compressed_content)
        return decompressed_content.decode('utf-8')
    except ClientError as e:
        logger.error(f"Failed to download s3://{bucket}/{key}: {e}")
        raise


def process_cur_manifest(manifest: dict[str, Any]) -> None:
    """
    Process a CUR manifest: download CSV, transform, and generate output files.
    """
    logger.info(f"Processing CUR manifest: {manifest.get('reportId')}")
    
    s3_client = get_s3_client()
    
    # Process each report key in the manifest
    for report_key in manifest.get('reportKeys', []):
        logger.info(f"Downloading: {report_key}")
        
        # Download and decompress CSV
        csv_text = download_and_decompress(s3_client, RAW_BUCKET, report_key)
        
        # Parse CSV
        lines = [line for line in csv_text.split('\n') if line.strip()]
        if len(lines) < 2:
            logger.info("No data rows in CSV")
            continue
        
        headers = parse_csv_line(lines[0])
        logger.info(f"CSV headers (first 10): {', '.join(headers[:10])}")
        logger.info(f"All headers ({len(headers)}): {', '.join(headers)}")
        logger.info(f"Processing {len(lines) - 1} rows")
        
        # Transform records
        transformed_records = []
        for i in range(1, len(lines)):
            values = parse_csv_line(lines[i])
            if len(values) == len(headers):
                transformed_records.append(transform_cur_record(headers, values))
        
        logger.info(f"Transformed {len(transformed_records)} records")
        
        # DEBUG: Log sample of application tags
        sample_records = transformed_records[:5]
        logger.info('Sample records with tags: %s', json.dumps([
            {
                'applicationTag': r['applicationTag'],
                'service': r['service'],
                'cost': r['cost']
            } for r in sample_records
        ], indent=2))
        
        tagged_count = sum(1 for r in transformed_records if r.get('applicationTag') and r['applicationTag'] != 'uncategorized')
        untagged_count = sum(1 for r in transformed_records if not r.get('applicationTag') or r['applicationTag'] == 'uncategorized')
        logger.info(f"Tagged records: {tagged_count}, Untagged records: {untagged_count}")
        
        # Aggregate by date + service (for costs.json)
        aggregated = {}
        for record in transformed_records:
            date = record.get('timestamp', '').split('T')[0] or 'unknown'
            key = f"{date}_{record['service']}"
            
            if key not in aggregated:
                aggregated[key] = {
                    'date': date,
                    'service': record['service'],
                    'serviceName': record['serviceName'],
                    'region': record['region'],
                    'totalCost': 0.0,
                    'totalBlendedCost': 0.0,
                    'totalUsage': 0.0,
                    'recordCount': 0,
                }
            
            aggregated[key]['totalCost'] += record['cost']
            aggregated[key]['totalBlendedCost'] += record['blendedCost']
            aggregated[key]['totalUsage'] += record['usageAmount']
            aggregated[key]['recordCount'] += 1
        
        aggregated_array = list(aggregated.values())
        logger.info(f"Aggregated to {len(aggregated_array)} records")
        
        # Extract billing period
        # Handles both ISO format ("2026-07-01T00:00:00Z") and compact format
        # ("20260701T00:00:00.000Z") by stripping all non-digits and taking first 8.
        start_date = re.sub(r'[^0-9]', '', manifest.get('billingPeriod', {}).get('start', ''))[:8]
        end_date   = re.sub(r'[^0-9]', '', manifest.get('billingPeriod', {}).get('end',   ''))[:8]
        if not start_date or len(start_date) < 6:
            logger.error("Could not parse billingPeriod.start from manifest; skipping")
            continue
        billing_period_path = f"{start_date}-{end_date}"
        
        # Generate daily-costs.json
        daily_costs = aggregate_daily_costs(transformed_records)
        monthly_total = sum(d['totalCost'] for d in daily_costs)
        daily_costs_output = {
            'billingPeriod': {'start': start_date, 'end': end_date},
            'dailyCosts': daily_costs,
            'monthlyTotal': round(monthly_total, 6),
            'generatedAt': datetime.utcnow().isoformat() + 'Z',
        }
        
        with open('daily-costs.json', 'w') as f:
            json.dump(daily_costs_output, f, indent=2)
        logger.info("Generated daily-costs.json")
        
        # Generate costs.json (granular data)
        with open('costs.json', 'w') as f:
            json.dump(aggregated_array, f, indent=2)
        logger.info("Generated costs.json")
        
        # Generate costs-by-tag.json
        costs_by_tag = aggregate_costs_by_tag(transformed_records)
        costs_by_tag['billingPeriod'] = {'start': start_date, 'end': end_date}
        with open('costs-by-tag.json', 'w') as f:
            json.dump(costs_by_tag, f, indent=2)
        logger.info("Generated costs-by-tag.json")
        
        # Generate summary.json (monthly totals per service)
        service_monthly = {}
        for record in transformed_records:
            service_key = record['service']
            if service_key not in service_monthly:
                service_monthly[service_key] = {
                    'service': service_key,
                    'serviceName': record['serviceName'],
                    'cost': 0.0,
                }
            service_monthly[service_key]['cost'] += record['cost']
        
        services_rounded = [
            {'service': s['service'], 'serviceName': s['serviceName'], 'cost': round(s['cost'], 6)}
            for s in service_monthly.values()
        ]
        summary = {
            'month': f"{start_date[:4]}-{start_date[4:6]}",
            'totalCost': round(sum(s['cost'] for s in service_monthly.values()), 6),
            'services': sorted(services_rounded, key=lambda x: x['cost'], reverse=True),
        }
        
        with open('summary.json', 'w') as f:
            json.dump([summary], f, indent=2)
        logger.info("Generated summary.json")
        
        logger.info(f"CUR processing completed for billing period: {billing_period_path}")


def process_latest_cur() -> None:
    """Find and process the latest CUR manifest from S3."""
    logger.info("Starting CUR processing...")
    
    s3_client = get_s3_client()
    
    # List objects to find manifest files
    try:
        response = s3_client.list_objects_v2(
            Bucket=RAW_BUCKET,
            Prefix='awscost/awscost/',
            MaxKeys=100
        )
    except ClientError as e:
        logger.error(f"Failed to list S3 objects: {e}")
        return
    
    if 'Contents' not in response or not response['Contents']:
        logger.info("No files found")
        return
    
    # Filter manifest files and sort by last modified
    manifest_files = [
        obj for obj in response['Contents']
        if obj.get('Key', '').lower().endswith('manifest.json')
    ]
    manifest_files.sort(key=lambda x: x.get('LastModified', datetime.min), reverse=True)
    
    if not manifest_files:
        logger.info("No manifest files found")
        return
    
    latest_manifest_key = manifest_files[0]['Key']
    logger.info(f"Processing manifest: {latest_manifest_key}")
    
    # Download and parse manifest
    try:
        manifest_response = s3_client.get_object(Bucket=RAW_BUCKET, Key=latest_manifest_key)
        manifest_text = manifest_response['Body'].read().decode('utf-8')
        manifest = json.loads(manifest_text)
    except Exception as e:
        logger.error(f"Failed to download/parse manifest: {e}")
        return
    
    process_cur_manifest(manifest)
    logger.info("CUR processing completed successfully")


def main():
    """Main entry point."""
    try:
        # Check if manifest data is provided via environment or argument
        if len(sys.argv) > 1:
            # Manifest JSON provided as argument
            manifest_path = sys.argv[1]
            with open(manifest_path) as f:
                manifest = json.load(f)
            process_cur_manifest(manifest)
        else:
            # Process latest CUR from S3
            process_latest_cur()
        
        logger.info("CUR processor completed successfully")
        return 0
    except Exception as e:
        logger.error(f"CUR processor error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())