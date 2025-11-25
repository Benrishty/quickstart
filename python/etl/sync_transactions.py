#!/usr/bin/env python3
"""
Sync transactions from Plaid to PostgreSQL database.
Uses incremental sync with cursors - only fetches new/modified transactions.

Usage:
    python etl/sync_transactions.py
"""

import os
import sys
import json
import time
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Configuration
PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'sandbox')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

# Initialize Plaid client
host = plaid.Environment.Sandbox
if PLAID_ENV == 'production':
    host = plaid.Environment.Production

configuration = plaid.Configuration(
    host=host,
    api_key={
        'clientId': PLAID_CLIENT_ID,
        'secret': PLAID_SECRET,
        'plaidVersion': '2020-09-14'
    }
)
api_client = plaid.ApiClient(configuration)
plaid_client = plaid_api.PlaidApi(api_client)


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )


def get_all_items():
    """Get all items, excluding those with errors that need re-authentication"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT item_id, access_token, error
        FROM plaid_items
        WHERE error IS NULL
           OR (error->>'error_code' NOT IN ('ITEM_LOGIN_REQUIRED', 'USER_PERMISSION_REVOKED', 'PENDING_EXPIRATION'))
    ''')
    items = cur.fetchall()
    cur.close()
    conn.close()
    return items


def get_items_needing_reauth():
    """Get items that need user re-authentication"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT pi.item_id, pi.error, i.name as institution_name
        FROM plaid_items pi
        LEFT JOIN institutions i ON pi.institution_id = i.institution_id
        WHERE pi.error IS NOT NULL
          AND pi.error->>'error_code' IN ('ITEM_LOGIN_REQUIRED', 'USER_PERMISSION_REVOKED', 'PENDING_EXPIRATION')
    ''')
    items = cur.fetchall()
    cur.close()
    conn.close()
    return items


def update_item_error(item_id, error_data):
    """Update item with error information"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        UPDATE plaid_items
        SET error = %s, updated_at = CURRENT_TIMESTAMP
        WHERE item_id = %s
    ''', (json.dumps(error_data, default=str), item_id))
    conn.commit()
    cur.close()
    conn.close()


def get_sync_cursor(conn, item_id):
    cur = conn.cursor()
    cur.execute('SELECT cursor FROM sync_cursors WHERE item_id = %s', (item_id,))
    result = cur.fetchone()
    cur.close()
    return result['cursor'] if result else ''


def save_sync_cursor(conn, item_id, cursor):
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO sync_cursors (item_id, cursor, last_synced_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (item_id) DO UPDATE SET
            cursor = EXCLUDED.cursor,
            last_synced_at = CURRENT_TIMESTAMP
    ''', (item_id, cursor))
    conn.commit()
    cur.close()


def save_transaction(conn, txn_data):
    cur = conn.cursor()

    location = txn_data.get('location', {}) or {}
    payment_meta = txn_data.get('payment_meta', {}) or {}
    personal_finance_category = txn_data.get('personal_finance_category', {}) or {}

    cur.execute('''
        INSERT INTO financial_transactions (
            transaction_id, account_id, amount, iso_currency_code, unofficial_currency_code,
            date, datetime, authorized_date, authorized_datetime,
            name, merchant_name, merchant_entity_id, logo_url, website,
            payment_channel, pending, pending_transaction_id, account_owner,
            transaction_code, transaction_type, category_id,
            personal_finance_category_primary, personal_finance_category_detailed,
            personal_finance_category_confidence, personal_finance_category_icon_url,
            location_address, location_city, location_region, location_postal_code,
            location_country, location_lat, location_lon, location_store_number,
            payment_meta_reference_number, payment_meta_ppd_id, payment_meta_payee,
            payment_meta_by_order_of, payment_meta_payer, payment_meta_payment_method,
            payment_meta_payment_processor, payment_meta_reason,
            counterparties, raw_data
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (transaction_id) DO UPDATE SET
            amount = EXCLUDED.amount,
            name = EXCLUDED.name,
            merchant_name = EXCLUDED.merchant_name,
            pending = EXCLUDED.pending,
            personal_finance_category_primary = EXCLUDED.personal_finance_category_primary,
            personal_finance_category_detailed = EXCLUDED.personal_finance_category_detailed,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
    ''', (
        txn_data.get('transaction_id'),
        txn_data.get('account_id'),
        txn_data.get('amount'),
        txn_data.get('iso_currency_code'),
        txn_data.get('unofficial_currency_code'),
        txn_data.get('date'),
        txn_data.get('datetime'),
        txn_data.get('authorized_date'),
        txn_data.get('authorized_datetime'),
        txn_data.get('name'),
        txn_data.get('merchant_name'),
        txn_data.get('merchant_entity_id'),
        txn_data.get('logo_url'),
        txn_data.get('website'),
        txn_data.get('payment_channel'),
        txn_data.get('pending'),
        txn_data.get('pending_transaction_id'),
        txn_data.get('account_owner'),
        txn_data.get('transaction_code'),
        txn_data.get('transaction_type'),
        txn_data.get('category_id'),
        personal_finance_category.get('primary'),
        personal_finance_category.get('detailed'),
        personal_finance_category.get('confidence_level'),
        personal_finance_category.get('icon_url'),
        location.get('address'),
        location.get('city'),
        location.get('region'),
        location.get('postal_code'),
        location.get('country'),
        location.get('lat'),
        location.get('lon'),
        location.get('store_number'),
        payment_meta.get('reference_number'),
        payment_meta.get('ppd_id'),
        payment_meta.get('payee'),
        payment_meta.get('by_order_of'),
        payment_meta.get('payer'),
        payment_meta.get('payment_method'),
        payment_meta.get('payment_processor'),
        payment_meta.get('reason'),
        json.dumps(txn_data.get('counterparties'), default=str) if txn_data.get('counterparties') else None,
        json.dumps(txn_data, default=str)
    ))
    conn.commit()
    cur.close()


def delete_transaction(conn, transaction_id):
    cur = conn.cursor()
    cur.execute('DELETE FROM financial_transactions WHERE transaction_id = %s', (transaction_id,))
    conn.commit()
    cur.close()


def sync_transactions():
    print(f"[{datetime.now().isoformat()}] Starting transaction sync")

    # Check for items needing re-authentication
    items_needing_reauth = get_items_needing_reauth()
    if items_needing_reauth:
        print(f"[{datetime.now().isoformat()}] WARNING: {len(items_needing_reauth)} items need re-authentication:")
        for item in items_needing_reauth:
            error = item.get('error', {})
            print(f"  - {item.get('institution_name', 'Unknown')} ({item['item_id'][:20]}...): {error.get('error_code')}")

    items = get_all_items()
    print(f"[{datetime.now().isoformat()}] Found {len(items)} healthy items to sync")

    total_added = 0
    total_modified = 0
    total_removed = 0
    items_failed = []

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        print(f"[{datetime.now().isoformat()}] Syncing item: {item_id}")

        conn = get_db_connection()
        cursor = get_sync_cursor(conn, item_id)

        added = []
        modified = []
        removed = []
        has_more = True

        try:
            while has_more:
                txn_request = TransactionsSyncRequest(
                    access_token=access_token,
                    cursor=cursor,
                )
                response = plaid_client.transactions_sync(txn_request).to_dict()
                cursor = response['next_cursor']

                if cursor == '':
                    time.sleep(2)
                    continue

                added.extend(response['added'])
                modified.extend(response['modified'])
                removed.extend(response['removed'])
                has_more = response['has_more']

            for txn in added:
                save_transaction(conn, txn)
            for txn in modified:
                save_transaction(conn, txn)
            for txn in removed:
                delete_transaction(conn, txn['transaction_id'])

            if cursor:
                save_sync_cursor(conn, item_id, cursor)

            total_added += len(added)
            total_modified += len(modified)
            total_removed += len(removed)

            print(f"[{datetime.now().isoformat()}] Item {item_id}: +{len(added)} added, ~{len(modified)} modified, -{len(removed)} removed")

        except plaid.ApiException as e:
            error_body = json.loads(e.body)
            error_code = error_body.get('error_code')
            print(f"[{datetime.now().isoformat()}] ERROR: Plaid API error for item {item_id}: {error_code}")

            # Store the error in the database for tracking
            update_item_error(item_id, error_body)
            items_failed.append({'item_id': item_id, 'error': error_code})

        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ERROR: {e}")
            items_failed.append({'item_id': item_id, 'error': str(e)})
        finally:
            conn.close()

    print(f"[{datetime.now().isoformat()}] Transaction sync complete. Total: +{total_added} added, ~{total_modified} modified, -{total_removed} removed")

    if items_failed:
        print(f"[{datetime.now().isoformat()}] WARNING: {len(items_failed)} items failed during sync")

    return {
        'added': total_added,
        'modified': total_modified,
        'removed': total_removed,
        'items_synced': len(items) - len(items_failed),
        'items_failed': len(items_failed),
        'items_needing_reauth': len(items_needing_reauth)
    }


if __name__ == '__main__':
    result = sync_transactions()
    print(json.dumps(result, indent=2))
