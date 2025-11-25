#!/usr/bin/env python3
"""
Sync account information from Plaid to PostgreSQL database.

Usage:
    python etl/sync_accounts.py
"""

import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import plaid
from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'sandbox')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

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
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT item_id, access_token FROM plaid_items')
    items = cur.fetchall()
    cur.close()
    conn.close()
    return items


def save_account(conn, account_data, item_id):
    cur = conn.cursor()
    balances = account_data.get('balances', {})

    cur.execute('''
        INSERT INTO financial_accounts (
            account_id, item_id, name, official_name, type, subtype, mask,
            current_balance, available_balance, limit_amount,
            iso_currency_code, unofficial_currency_code, persistent_account_id, raw_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_id) DO UPDATE SET
            name = EXCLUDED.name,
            official_name = EXCLUDED.official_name,
            type = EXCLUDED.type,
            subtype = EXCLUDED.subtype,
            mask = EXCLUDED.mask,
            current_balance = EXCLUDED.current_balance,
            available_balance = EXCLUDED.available_balance,
            limit_amount = EXCLUDED.limit_amount,
            iso_currency_code = EXCLUDED.iso_currency_code,
            unofficial_currency_code = EXCLUDED.unofficial_currency_code,
            persistent_account_id = EXCLUDED.persistent_account_id,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
    ''', (
        account_data.get('account_id'),
        item_id,
        account_data.get('name'),
        account_data.get('official_name'),
        account_data.get('type'),
        account_data.get('subtype'),
        account_data.get('mask'),
        balances.get('current'),
        balances.get('available'),
        balances.get('limit'),
        balances.get('iso_currency_code'),
        balances.get('unofficial_currency_code'),
        account_data.get('persistent_account_id'),
        json.dumps(account_data, default=str)
    ))
    conn.commit()
    cur.close()


def sync_accounts():
    print(f"[{datetime.now().isoformat()}] Starting account sync")

    items = get_all_items()
    print(f"[{datetime.now().isoformat()}] Found {len(items)} items to sync")

    total_accounts = 0

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        print(f"[{datetime.now().isoformat()}] Syncing accounts for item: {item_id}")

        conn = get_db_connection()

        try:
            accounts_request = AccountsGetRequest(access_token=access_token)
            response = plaid_client.accounts_get(accounts_request).to_dict()

            for account in response['accounts']:
                save_account(conn, account, item_id)
                total_accounts += 1

            print(f"[{datetime.now().isoformat()}] Item {item_id}: Synced {len(response['accounts'])} accounts")

        except plaid.ApiException as e:
            print(f"[{datetime.now().isoformat()}] ERROR: Plaid API error for item {item_id}: {e}")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ERROR: {e}")
        finally:
            conn.close()

    print(f"[{datetime.now().isoformat()}] Account sync complete. Synced {total_accounts} accounts")

    return {'accounts_synced': total_accounts}


if __name__ == '__main__':
    result = sync_accounts()
    print(json.dumps(result, indent=2))
