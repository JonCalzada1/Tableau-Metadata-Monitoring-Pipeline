import os
import csv
import sqlite3
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------
# Configuration
# --------------------------------------------------
TABLEAU_SERVER = os.getenv('TABLEAU_SERVER')
TABLEAU_SITE_CONTENT_URL = os.getenv('TABLEAU_SITE_CONTENT_URL', '')
TABLEAU_PAT_NAME = os.getenv('TABLEAU_PAT_NAME')
TABLEAU_PAT_SECRET = os.getenv('TABLEAU_PAT_SECRET')
TABLEAU_API_VERSION = os.getenv('TABLEAU_API_VERSION', '3.19')

DB_PATH = 'bi_metadata.db'
CSV_EXPORT_PATH = 'bi_assets_metadata.csv'
PAGE_SIZE = 100

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --------------------------------------------------
# Validation
# --------------------------------------------------
def validate_config() -> None:
    required_vars = {
        'TABLEAU_SERVER': TABLEAU_SERVER,
        'TABLEAU_PAT_NAME': TABLEAU_PAT_NAME,
        'TABLEAU_PAT_SECRET': TABLEAU_PAT_SECRET,
    }

    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        raise ValueError(f'Missing required environment variables: {", ".join(missing)}')

# --------------------------------------------------
# Utility Helpers
# --------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_get(d: Any, key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default

def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None

# --------------------------------------------------
# Tableau API Functions
# --------------------------------------------------
def sign_in() -> Dict[str, str]:
    '''
    Authenticate to Tableau REST API using Personal Access Token.
    Returns auth token and site id.
    '''
    url = f'{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/auth/signin'

    payload = {
        'credentials': {
            'personalAccessTokenName': TABLEAU_PAT_NAME,
            'personalAccessTokenSecret': TABLEAU_PAT_SECRET,
            'site': {
                'contentUrl': TABLEAU_SITE_CONTENT_URL
            }
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        credentials = data.get('credentials', {})
        token = credentials.get('token')
        site_id = safe_get(credentials.get('site', {}), 'id')

        if not token or not site_id:
            raise ValueError('Authentication succeeded but token or site_id was missing.')

        logging.info('Successfully authenticated to Tableau API.')
        return {'token': token, 'site_id': site_id}

    except (requests.RequestException, ValueError) as e:
        logging.error(f'Failed to authenticate with Tableau API: {e}')
        raise

def sign_out(token: str) -> None:
    '''
    Sign out from Tableau session.
    '''
    url = f'{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/auth/signout'
    headers = {'X-Tableau-Auth': token}

    try:
        response = requests.post(url, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info('Signed out from Tableau API.')
    except requests.RequestException as e:
        logging.warning(f'Sign out failed: {e}')

def tableau_get(token: str, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    '''
    Reusable GET request helper for Tableau API.
    '''
    headers = {
        'X-Tableau-Auth': token,
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f'Tableau GET request failed for {url}: {e}')
        raise

def fetch_paginated_items(
    token: str,
    site_id: str,
    endpoint: str,
    root_key: str,
    item_key: str
) -> List[Dict[str, Any]]:
    '''
    Generic pagination fetcher for Tableau endpoints.
    '''
    all_items = []
    page_number = 1

    while True:
        url = f'{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/sites/{site_id}/{endpoint}'
        params = {
            'pageSize': PAGE_SIZE,
            'pageNumber': page_number
        }

        data = tableau_get(token, url, params=params)
        root = data.get(root_key, {})
        items = root.get(item_key, [])

        if isinstance(items, dict):
            items = [items]

        pagination = data.get('pagination', {})
        total_available = int(pagination.get('totalAvailable', 0))

        all_items.extend(items)

        logging.info(
            f'Fetched page {page_number} from {endpoint} '
            f'({len(items)} items, {len(all_items)}/{total_available} total).'
        )

        if len(all_items) >= total_available or len(items) == 0:
            break

        page_number += 1

    return all_items

def get_workbooks(token: str, site_id: str) -> List[Dict[str, Any]]:
    '''
    Fetch workbook metadata with pagination.
    '''
    workbooks = fetch_paginated_items(
        token=token,
        site_id=site_id,
        endpoint='workbooks',
        root_key='workbooks',
        item_key='workbook'
    )
    logging.info(f'Fetched {len(workbooks)} total workbooks.')
    return workbooks

def get_views(token: str, site_id: str) -> List[Dict[str, Any]]:
    '''
    Fetch view metadata with pagination.
    '''
    views = fetch_paginated_items(
        token=token,
        site_id=site_id,
        endpoint='views',
        root_key='views',
        item_key='view'
    )
    logging.info(f'Fetched {len(views)} total views.')
    return views

# --------------------------------------------------
# Business Logic / Transformation
# --------------------------------------------------
def derive_status(
    last_updated: Optional[str],
    views_last_30d: Optional[int],
    refresh_status: Optional[str],
    total_views: Optional[int]
) -> str:
    '''
    Derive a business-friendly status for the BI asset.
    '''
    if refresh_status and refresh_status.lower() == 'failed':
        return 'failing_refresh'

    if views_last_30d == 0:
        return 'unused'

    if views_last_30d is None and total_views == 0:
        return 'unused'

    updated_dt = parse_iso_datetime(last_updated)
    if updated_dt:
        age_days = (datetime.now(timezone.utc) - updated_dt).days
        if age_days > 90:
            return 'stale'

    return 'active'

def transform_workbooks(workbooks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    '''
    Transform raw Tableau workbook metadata into analytics-ready records.
    '''
    transformed = []
    synced_at = utc_now_iso()

    for wb in workbooks:
        owner = safe_get(wb, 'owner', {})
        project = safe_get(wb, 'project', {})

        total_views = safe_get(wb, 'viewCount')

        record = {
            'asset_id': safe_get(wb, 'id'),
            'asset_name': safe_get(wb, 'name'),
            'asset_type': 'workbook',
            'workbook_name': safe_get(wb, 'name'),
            'project_name': safe_get(project, 'name'),
            'owner_name': safe_get(owner, 'name'),
            'owner_id': safe_get(owner, 'id'),
            'last_updated': safe_get(wb, 'updatedAt'),
            'last_viewed': None,
            'views_last_30d': None,
            'total_views': total_views,
            'refresh_status': 'unknown',
            'status': None,
            'web_url': safe_get(wb, 'webpageUrl') or safe_get(wb, 'contentUrl'),
            'last_synced_at': synced_at
        }

        record['status'] = derive_status(
            last_updated=record['last_updated'],
            views_last_30d=record['views_last_30d'],
            refresh_status=record['refresh_status'],
            total_views=record['total_views']
        )

        if record['asset_id']:
            transformed.append(record)

    logging.info(f'Transformed {len(transformed)} workbook records.')
    return transformed

def normalize_key(value: Optional[str]) -> Optional[str]:
    '''
    Normalize strings for matching workbook names and content URL slugs.
    '''
    if not value:
        return None

    return ''.join(value.lower().split())


def build_workbook_lookup(workbooks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    '''
    Build a workbook lookup keyed by multiple workbook identifiers.
    '''
    lookup = {}

    for wb in workbooks:
        workbook_name = safe_get(wb, 'name')
        workbook_content_url = safe_get(wb, 'contentUrl')

        workbook_context = {
            'workbook_name': workbook_name,
            'project_name': safe_get(safe_get(wb, 'project', {}), 'name'),
            'owner_name': safe_get(safe_get(wb, 'owner', {}), 'name'),
            'owner_id': safe_get(safe_get(wb, 'owner', {}), 'id'),
            'web_url': safe_get(wb, 'webpageUrl') or workbook_content_url
        }

        possible_keys = {
            workbook_name,
            normalize_key(workbook_name),
            workbook_content_url,
            normalize_key(workbook_content_url)
        }

        for key in possible_keys:
            if key:
                lookup[key] = workbook_context

    return lookup

def infer_workbook_name_from_content_url(content_url: Optional[str]) -> Optional[str]:
    '''
    Infer workbook name from Tableau view content URL like:
    Superstore/sheets/Overview
    '''
    if not content_url:
        return None

    parts = content_url.split('/sheets/')
    if len(parts) == 2:
        return parts[0]

    return None

def transform_views(
    views: List[Dict[str, Any]],
    workbook_lookup: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    '''
    Transform raw Tableau view metadata into analytics-ready records.
    '''
    transformed = []
    synced_at = utc_now_iso()

    for view in views:
        owner = safe_get(view, 'owner', {})
        workbook = safe_get(view, 'workbook', {})
        project = safe_get(view, 'project', {})

        content_url = safe_get(view, 'contentUrl')
        nested_workbook_name = safe_get(workbook, 'name')
        inferred_workbook_name = infer_workbook_name_from_content_url(content_url)

        lookup_candidates = [
            nested_workbook_name,
            normalize_key(nested_workbook_name),
            inferred_workbook_name,
            normalize_key(inferred_workbook_name)
        ]

        workbook_context = {}
        for candidate in lookup_candidates:
            if candidate and candidate in workbook_lookup:
                workbook_context = workbook_lookup[candidate]
                break

        workbook_name = (
            safe_get(workbook_context, 'workbook_name')
            or nested_workbook_name
            or inferred_workbook_name
        )

        total_views = safe_get(view, 'viewCount')

        owner_name = safe_get(owner, 'name') or workbook_context.get('owner_name')
        owner_id = safe_get(owner, 'id') or workbook_context.get('owner_id')
        project_name = safe_get(project, 'name') or workbook_context.get('project_name')

        record = {
            'asset_id': safe_get(view, 'id'),
            'asset_name': safe_get(view, 'name'),
            'asset_type': 'view',
            'workbook_name': workbook_name,
            'project_name': project_name,
            'owner_name': owner_name,
            'owner_id': owner_id,
            'last_updated': safe_get(view, 'updatedAt'),
            'last_viewed': safe_get(view, 'lastViewedAt'),
            'views_last_30d': safe_get(view, 'viewsLast30d'),
            'total_views': safe_get(view, 'viewCount'),
            'refresh_status': 'unknown',
            'status': None,
            'web_url': content_url,
            'last_synced_at': synced_at
        }

        record['status'] = derive_status(
            last_updated=record['last_updated'],
            views_last_30d=record['views_last_30d'],
            refresh_status=record['refresh_status'],
            total_views=record['total_views']
        )

        if record['asset_id']:
            transformed.append(record)

    logging.info(f'Transformed {len(transformed)} view records.')
    return transformed

# --------------------------------------------------
# SQLite Functions
# --------------------------------------------------
def init_db(db_path: str) -> None:
    '''
    Create table if it does not exist.
    '''
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bi_assets_metadata (
            asset_id TEXT PRIMARY KEY,
            asset_name TEXT,
            asset_type TEXT,
            workbook_name TEXT,
            project_name TEXT,
            owner_name TEXT,
            owner_id TEXT,
            last_updated TEXT,
            last_viewed TEXT,
            views_last_30d INTEGER,
            total_views INTEGER,
            refresh_status TEXT,
            status TEXT,
            web_url TEXT,
            last_synced_at TEXT
        )
    ''')

    conn.commit()
    conn.close()
    logging.info('Initialized SQLite database.')

def upsert_assets(db_path: str, records: List[Dict[str, Any]]) -> None:
    '''
    Insert or update metadata records in SQLite.
    '''
    if not records:
        logging.warning('No records to upsert.')
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = '''
        INSERT INTO bi_assets_metadata (
            asset_id, asset_name, asset_type, workbook_name, project_name,
            owner_name, owner_id, last_updated, last_viewed, views_last_30d,
            total_views, refresh_status, status, web_url, last_synced_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(asset_id) DO UPDATE SET
            asset_name = excluded.asset_name,
            asset_type = excluded.asset_type,
            workbook_name = excluded.workbook_name,
            project_name = excluded.project_name,
            owner_name = excluded.owner_name,
            owner_id = excluded.owner_id,
            last_updated = excluded.last_updated,
            last_viewed = excluded.last_viewed,
            views_last_30d = excluded.views_last_30d,
            total_views = excluded.total_views,
            refresh_status = excluded.refresh_status,
            status = excluded.status,
            web_url = excluded.web_url,
            last_synced_at = excluded.last_synced_at
    '''

    values = [
        (
            r['asset_id'],
            r['asset_name'],
            r['asset_type'],
            r['workbook_name'],
            r['project_name'],
            r['owner_name'],
            r['owner_id'],
            r['last_updated'],
            r['last_viewed'],
            r['views_last_30d'],
            r['total_views'],
            r['refresh_status'],
            r['status'],
            r['web_url'],
            r['last_synced_at']
        )
        for r in records
    ]

    try:
        cursor.executemany(query, values)
        conn.commit()
        logging.info(f'Upserted {len(records)} records into SQLite.')
    except sqlite3.Error as e:
        logging.error(f'Failed to upsert records: {e}')
        raise
    finally:
        conn.close()

# --------------------------------------------------
# CSV Export
# --------------------------------------------------
def export_to_csv(records: List[Dict[str, Any]], file_path: str) -> None:
    '''
    Export transformed records to CSV for easy inspection/sharing.
    '''
    if not records:
        logging.warning('No records to export to CSV.')
        return

    fieldnames = list(records[0].keys())

    try:
        with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        logging.info(f'Exported {len(records)} records to CSV at {file_path}.')
    except OSError as e:
        logging.error(f'Failed to export CSV: {e}')
        raise

# --------------------------------------------------
# Main Pipeline
# --------------------------------------------------
def main():
    validate_config()
    init_db(DB_PATH)

    auth = sign_in()
    token = auth['token']
    site_id = auth['site_id']

    try:
        workbooks = get_workbooks(token, site_id)
        views = get_views(token, site_id)

        workbook_records = transform_workbooks(workbooks)
        workbook_lookup = build_workbook_lookup(workbooks)
        view_records = transform_views(views, workbook_lookup)

        all_records = workbook_records + view_records

        upsert_assets(DB_PATH, all_records)
        export_to_csv(all_records, CSV_EXPORT_PATH)

        logging.info('Pipeline completed successfully.')

    finally:
        sign_out(token)

if __name__ == '__main__':
    main()