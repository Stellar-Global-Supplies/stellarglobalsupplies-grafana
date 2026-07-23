"""
CloudFront Web Traffic Processor

Processes CloudFront access logs from S3 and generates web traffic analytics.
Outputs a single JSON report with 7-day rolling window metrics.

Environment variables:
  LOGS_BUCKET          - S3 bucket containing CloudFront logs (default: stellarglobal-cf-logs)
  AWS_REGION           - AWS region (default: us-east-1)
"""

import json
import os
import sys
import gzip
import io
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Any

import boto3

# ── Logging setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

# ── Configuration ─────────────────────────────────────────────────────────────
REGION     = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET     = os.environ.get('LOGS_BUCKET', 'stellarglobal-cf-logs')
PERIOD_DAYS = 7  # Rolling 7-day window

# ── AWS clients ───────────────────────────────────────────────────────────────
s3_client = boto3.client('s3', region_name=REGION)


# ── Device type from User-Agent ───────────────────────────────────────────────
def get_device(ua: str) -> str:
    """Classify device type from User-Agent string."""
    ua = (ua or '').lower()
    if 'mobile' in ua or 'android' in ua or 'iphone' in ua:
        return 'Mobile'
    if 'tablet' in ua or 'ipad' in ua:
        return 'Tablet'
    return 'Desktop'


# ── Extract browser from User-Agent ──────────────────────────────────────────
def get_browser(ua: str) -> str:
    """Extract browser name from User-Agent string."""
    ua = (ua or '').lower()
    if 'edg/' in ua or 'edge/' in ua:
        return 'Edge'
    if 'chrome/' in ua and 'chromium/' not in ua:
        return 'Chrome'
    if 'safari/' in ua and 'chrome/' not in ua:
        return 'Safari'
    if 'firefox/' in ua:
        return 'Firefox'
    if 'opera/' in ua or 'opr/' in ua:
        return 'Opera'
    if 'msie/' in ua or 'trident/' in ua:
        return 'IE'
    return 'Other'


# ── Extract OS from User-Agent ───────────────────────────────────────────────
def get_os(ua: str) -> str:
    """Extract operating system from User-Agent string."""
    ua = (ua or '').lower()
    if 'windows' in ua:
        return 'Windows'
    if 'mac os x' in ua or 'macintosh' in ua:
        return 'macOS'
    if 'linux' in ua and 'android' not in ua:
        return 'Linux'
    if 'android' in ua:
        return 'Android'
    if 'iphone' in ua or 'ipad' in ua or 'ios' in ua:
        return 'iOS'
    if 'chrome os' in ua or 'cros' in ua:
        return 'Chrome OS'
    return 'Other'


# ── Parse one JSON log line ───────────────────────────────────────────────────
def parse_record(raw: str) -> dict[str, Any] | None:
    """Parse a single CloudFront JSON log line."""
    try:
        r = json.loads(raw)

        def g(*keys):
            for k in keys:
                if k in r:
                    return r[k]
            return ''

        ts_raw = g('timestamp', 'date', 'dateTime')
        uri    = g('cs-uri-stem', 'csUriStem', 'uri-stem', 'uriStem') or '/'
        ua     = g('cs-user-agent', 'csUserAgent', 'userAgent')
        ip     = g('c-ip', 'cIp', 'clientIp')
        loc    = g('x-edge-location', 'xEdgeLocation', 'edgeLocation')
        
        # Additional fields
        status = g('sc-status', 'status', 'statusCode')
        sc_bytes = g('sc-bytes', 'responseSize', 'bytes')
        cs_bytes = g('cs-bytes', 'requestSize')
        method = g('cs-method', 'method', 'httpMethod')
        referer = g('cs-referer', 'referer', 'referrer')
        result_type = g('x-edge-result-type', 'resultType')
        time_taken = g('time-taken', 'timeTaken', 'responseTime')
        protocol = g('cs-protocol', 'protocol')

        # Parse timestamp into date + hour
        rec_date = ''
        rec_hour = -1
        if ts_raw:
            try:
                # Unix epoch integer/float
                ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                rec_date = ts.strftime('%Y-%m-%d')
                rec_hour = ts.hour
            except (ValueError, OSError):
                try:
                    # ISO string e.g. "2026-03-28T04:12:00Z"
                    ts = datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                    rec_date = ts.strftime('%Y-%m-%d')
                    rec_hour = ts.hour
                except Exception:
                    pass

        return {
            'date': rec_date,
            'hour': rec_hour,
            'uri':  uri.split('?')[0] or '/',   # strip query string
            'ua':   ua,
            'ip':   ip,
            'loc':  loc,
            'status': int(status) if status and str(status).isdigit() else 0,
            'sc_bytes': int(sc_bytes) if sc_bytes and str(sc_bytes).isdigit() else 0,
            'cs_bytes': int(cs_bytes) if cs_bytes and str(cs_bytes).isdigit() else 0,
            'method': (method or 'GET').upper(),
            'referer': referer or '',
            'result_type': result_type or '',
            'time_taken': float(time_taken) if time_taken and str(time_taken).replace('.', '').isdigit() else 0.0,
            'protocol': (protocol or '').upper(),
            'browser': get_browser(ua),
            'os': get_os(ua),
        }
    except Exception:
        return None


# ── Extract date + hour from S3 key filename ──────────────────────────────────
def date_from_key(key: str) -> tuple[str, int]:
    """
    Extract date and hour from S3 key filename.
    Key format: AWSLogs/471112840461/CloudFront/E33O5N0T0HY2K9.2026-03-28-04.98a5c184.gz
    """
    try:
        fname     = key.split('/')[-1]      # E33O5N0T0HY2K9.2026-03-28-04.98a5c184.gz
        parts     = fname.split('.')        # ['E33O5N0T0HY2K9', '2026-03-28-04', '98a5c184', 'gz']
        date_part = parts[1]               # '2026-03-28-04'
        date_str  = date_part[:10]         # '2026-03-28'
        hour      = int(date_part[11:13]) if len(date_part) > 10 else -1
        return date_str, hour
    except Exception:
        return '', -1


# ── List all .gz log files in bucket on or after cutoff_date ─────────────────
def list_log_keys(bucket: str, cutoff_date: str) -> list[str]:
    """List all CloudFront log files since cutoff_date."""
    keys      = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='AWSLogs/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.gz'):
                continue
            file_date, _ = date_from_key(key)
            if file_date and file_date >= cutoff_date:
                keys.append(key)
            elif not file_date:
                # Can't determine date from key — include and filter after parsing
                keys.append(key)
    logger.info(f"Found {len(keys)} log files since {cutoff_date}")
    return keys


# ── Download, decompress, and parse one log file ──────────────────────────────
def parse_log_file(bucket: str, key: str) -> list[dict]:
    """Download and parse a single gzipped CloudFront log file."""
    records = []
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        raw = obj['Body'].read()
        with gzip.open(io.BytesIO(raw), 'rt', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                rec = parse_record(line)
                if rec:
                    # Use filename date as fallback if record timestamp didn't parse
                    if not rec['date'] or rec['hour'] < 0:
                        rec['date'], rec['hour'] = date_from_key(key)
                    records.append(rec)
    except Exception as e:
        logger.error(f"Error parsing {key}: {e}")
    return records


# ── Map CloudFront edge location code → country ───────────────────────────────
EDGE_COUNTRY = {
    # India
    'BOM': 'India', 'MAA': 'India', 'DEL': 'India',
    'HYD': 'India', 'CCU': 'India', 'BLR': 'India',
    # Middle East
    'DXB': 'UAE',   'AUH': 'UAE',   'BAH': 'Bahrain',
    # Southeast Asia
    'SIN': 'Singapore', 'KUL': 'Malaysia', 'BKK': 'Thailand',
    'CGK': 'Indonesia',
    # East Asia
    'NRT': 'Japan',  'KIX': 'Japan',
    'ICN': 'South Korea', 'HKG': 'Hong Kong',
    # Europe
    'LHR': 'UK',    'MAN': 'UK',
    'CDG': 'France', 'FRA': 'Germany', 'AMS': 'Netherlands',
    'MXP': 'Italy',  'MAD': 'Spain',   'ARN': 'Sweden',
    # USA
    'JFK': 'USA',   'LAX': 'USA',   'ORD': 'USA',
    'IAD': 'USA',   'DFW': 'USA',   'SFO': 'USA',
    'ATL': 'USA',   'BOS': 'USA',   'MIA': 'USA',
    'SEA': 'USA',
    # Oceania
    'SYD': 'Australia', 'MEL': 'Australia', 'AKL': 'New Zealand',
    # Americas
    'GRU': 'Brazil', 'GIG': 'Brazil',
    'BOG': 'Colombia', 'SCL': 'Chile',
    # Africa
    'CAI': 'Egypt',  'JNB': 'South Africa', 'LOS': 'Nigeria',
}


def loc_to_country(loc: str) -> str:
    """Convert CloudFront edge location code to country name."""
    if not loc:
        return 'Unknown'
    code = loc[:3].upper()
    return EDGE_COUNTRY.get(code, f'Other ({code})')


# ── Aggregate records into a report dict ─────────────────────────────────────
def build_report(records: list[dict], period_days: int) -> dict[str, Any] | None:
    """Build web traffic analytics report from parsed records."""
    total = len(records)
    if total == 0:
        return None

    page_counts      = defaultdict(int)
    country_hits     = defaultdict(int)
    device_hits      = defaultdict(int)
    hour_hits        = defaultdict(int)
    date_hits        = defaultdict(int)
    ip_set           = set()
    
    # New aggregations
    status_hits      = defaultdict(int)
    browser_hits     = defaultdict(int)
    os_hits          = defaultdict(int)
    method_hits      = defaultdict(int)
    referrer_hits    = defaultdict(int)
    cache_hits       = defaultdict(int)
    protocol_hits    = defaultdict(int)
    total_sc_bytes   = 0
    total_cs_bytes   = 0
    total_time_taken = 0.0

    for r in records:
        page_counts[r['uri']]                  += 1
        country_hits[loc_to_country(r['loc'])] += 1
        device_hits[get_device(r['ua'])]       += 1
        date_hits[r['date']]                   += 1
        if r['hour'] >= 0:
            hour_hits[r['hour']]               += 1
        if r['ip']:
            ip_set.add(r['ip'])
        
        # New metrics
        status = r.get('status', 0)
        status_hits[status]                    += 1
        
        browser_hits[r.get('browser', 'Other')] += 1
        os_hits[r.get('os', 'Other')]           += 1
        method_hits[r.get('method', 'GET')]     += 1
        referrer_hits[r.get('referer', '')]     += 1
        cache_hits[r.get('result_type', '')]    += 1
        protocol_hits[r.get('protocol', '')]    += 1
        
        total_sc_bytes   += r.get('sc_bytes', 0)
        total_cs_bytes   += r.get('cs_bytes', 0)
        total_time_taken += r.get('time_taken', 0.0)

    # Top pages
    top_pages = [
        {"page": p, "visits": v, "bounce_pct": 0}
        for p, v in sorted(page_counts.items(), key=lambda x: -x[1])[:10]
    ]

    # Geo distribution
    total_geo = sum(country_hits.values()) or 1
    geo_dist  = [
        {"country": c, "requests": v, "pct": round(v * 100 / total_geo)}
        for c, v in sorted(country_hits.items(), key=lambda x: -x[1])[:8]
    ]

    # Device split
    total_dev    = sum(device_hits.values()) or 1
    device_split = [
        {"device": d, "pct": round(v * 100 / total_dev)}
        for d, v in device_hits.items()
    ]

    # Traffic over time — every day in range, 0 if no hits
    now       = datetime.utcnow()
    cutoff_dt = now - timedelta(days=period_days)
    all_dates = [
        (cutoff_dt + timedelta(days=i)).strftime('%Y-%m-%d')
        for i in range(period_days + 1)
    ]
    traffic_over_time = [
        {"date": d, "requests": date_hits.get(d, 0)}
        for d in all_dates
    ]

    # Peak hours (0–23)
    peak_hours    = [{"hour": h, "requests": hour_hits.get(h, 0)} for h in range(24)]
    peak_hour_val = max(peak_hours, key=lambda x: x['requests'])['hour']

    # HTTP status distribution
    status_2xx = sum(v for k, v in status_hits.items() if 200 <= k < 300)
    status_3xx = sum(v for k, v in status_hits.items() if 300 <= k < 400)
    status_4xx = sum(v for k, v in status_hits.items() if 400 <= k < 500)
    status_5xx = sum(v for k, v in status_hits.items() if 500 <= k < 600)
    error_requests = status_4xx + status_5xx
    error_rate = round(error_requests * 100 / total, 1) if total > 0 else 0
    
    status_distribution = [
        {"status": "2xx", "count": status_2xx, "pct": round(status_2xx * 100 / total, 1)},
        {"status": "3xx", "count": status_3xx, "pct": round(status_3xx * 100 / total, 1)},
        {"status": "4xx", "count": status_4xx, "pct": round(status_4xx * 100 / total, 1)},
        {"status": "5xx", "count": status_5xx, "pct": round(status_5xx * 100 / total, 1)},
    ]

    # Cache distribution
    cache_hit_count = cache_hits.get('Hit', 0) + cache_hits.get('HitFromCloudFront', 0)
    cache_miss_count = cache_hits.get('Miss', 0)
    cache_total = cache_hit_count + cache_miss_count
    cache_hit_pct = round(cache_hit_count * 100 / cache_total, 1) if cache_total > 0 else 0

    # Browser distribution
    browser_dist = [
        {"browser": b, "pct": round(v * 100 / total, 1)}
        for b, v in sorted(browser_hits.items(), key=lambda x: -x[1])[:5]
    ]

    # OS distribution
    os_dist = [
        {"os": o, "pct": round(v * 100 / total, 1)}
        for o, v in sorted(os_hits.items(), key=lambda x: -x[1])[:5]
    ]

    # HTTP methods
    method_dist = [
        {"method": m, "pct": round(v * 100 / total, 1)}
        for m, v in sorted(method_hits.items(), key=lambda x: -x[1])
    ]

    # Top referrers
    top_referrers = [
        {"referer": ref, "visits": v}
        for ref, v in sorted(referrer_hits.items(), key=lambda x: -x[1])[:10]
        if ref  # exclude empty referrers
    ]

    # Protocol distribution
    https_count = protocol_hits.get('HTTPS', 0) + protocol_hits.get('HTTP/2', 0) + protocol_hits.get('HTTP/3', 0)
    https_pct = round(https_count * 100 / total, 1) if total > 0 else 0

    # Response metrics
    avg_response_size = round(total_sc_bytes / total) if total > 0 else 0
    avg_request_size = round(total_cs_bytes / total) if total > 0 else 0
    avg_response_time = round(total_time_taken / total, 3) if total > 0 else 0

    mobile_pct    = next((d['pct'] for d in device_split if d['device'] == 'Mobile'), 0)
    top_countries = [g['country'] for g in geo_dist[:3]]
    avg_daily     = total // period_days if period_days else 0
    high_intent   = sum(
        v for p, v in page_counts.items()
        if 'contact' in p or 'product' in p or 'quote' in p
    )

    return {
        "period":       "7-day",
        "label":        f"Last {period_days} Days",
        "generated_at": now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "summary": {
            "total_requests": total,
            "unique_ips":     len(ip_set),
            "avg_daily":      avg_daily,
            "top_country":    geo_dist[0]['country'] if geo_dist else 'N/A',
            "mobile_pct":     mobile_pct,
            "desktop_pct":    100 - mobile_pct,
            "bounce_rate":    0,
            "peak_hour":      f"{peak_hour_val:02d}:00",
            "error_rate":     error_rate,
            "cache_hit_pct":  cache_hit_pct,
            "https_pct":      https_pct,
        },
        "traffic_over_time": traffic_over_time,
        "top_pages":         top_pages,
        "geo_distribution":  geo_dist,
        "device_split":      device_split,
        "peak_hours":        peak_hours,
        "status_distribution": status_distribution,
        "browser_distribution": browser_dist,
        "os_distribution":   os_dist,
        "method_distribution": method_dist,
        "top_referrers":     top_referrers,
        "cache_distribution": {
            "hit_pct": cache_hit_pct,
            "miss_pct": 100 - cache_hit_pct,
        },
        "response_metrics": {
            "avg_response_size_bytes": avg_response_size,
            "avg_request_size_bytes": avg_request_size,
            "avg_response_time_seconds": avg_response_time,
            "total_bytes_transferred": total_sc_bytes,
        },
        "meta_insights": {
            "recommended_objective": "Traffic" if mobile_pct > 60 else "Awareness",
            "top_locations":         top_countries,
            "best_placement":        "Reels + Stories" if mobile_pct > 60 else "Feed + Right Column",
            "best_ad_time":          f"{peak_hour_val:02d}:00 – {(peak_hour_val + 3) % 24:02d}:00 UTC",
            "warm_audience_size":    int(total * 0.3),
            "high_intent_visits":    high_intent,
        },
    }


# ── Lambda entry point ────────────────────────────────────────────────────────
def handler(event: dict, context: Any) -> dict:
    """AWS Lambda handler - processes CloudFront logs and generates report."""
    # Handle API Gateway trigger (manual refresh from UI)
    if isinstance(event, dict) and 'requestContext' in event:
        http_method = event.get('requestContext', {}).get('http', {}).get('method', '')
        if http_method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type',
                },
                'body': '',
            }

    now     = datetime.utcnow()
    cutoff_date = (now - timedelta(days=PERIOD_DAYS)).strftime('%Y-%m-%d')
    logger.info(f"--- Building web-traffic-report.json (cutoff: {cutoff_date}) ---")

    log_keys    = list_log_keys(BUCKET, cutoff_date)
    all_records = []
    for lk in log_keys:
        all_records.extend(parse_log_file(BUCKET, lk))

    # Strict date filter after parsing
    records = [r for r in all_records if r['date'] >= cutoff_date]
    logger.info(f"    {len(all_records)} total records, {len(records)} within date range")

    report = build_report(records, PERIOD_DAYS)
    if not report:
        logger.warning("    No data — skipping report generation")
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'success': True,
                'message': 'No web traffic data available',
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
            }),
        }

    # Save report to JSON file
    with open('web-traffic-report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"    Saved web-traffic-report.json — {report['summary']['total_requests']} requests")

    # Return HTTP response for both scheduled and API Gateway invocations
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps({
            'success': True,
            'message': 'Web traffic report generated',
            'total_requests': report['summary']['total_requests'],
            'unique_ips': report['summary']['unique_ips'],
            'timestamp': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        }),
    }


# ── CLI entry point ───────────────────────────────────────────────────────────
def main():
    """CLI entry point for testing."""
    logger.info("Starting web traffic processor...")
    result = handler({}, None)
    logger.info(f"Processing complete: {result['body']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())