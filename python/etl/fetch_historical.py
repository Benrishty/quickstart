#!/usr/bin/env python3
"""
Fetch all historical transactions from Plaid.
Uses TransactionsGet API to fetch up to 5 years of data.

Usage:
    python etl/fetch_historical.py
"""

import os
import sys
import json
import time
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest

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


def fetch_historical(years_back=5):
    print(f"[{datetime.now().isoformat()}] Starting historical transaction fetch (going back {years_back} years)")

    items = get_all_items()
    print(f"[{datetime.now().isoformat()}] Found {len(items)} items to fetch")

    total_fetched = 0

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        print(f"[{datetime.now().isoformat()}] Fetching historical transactions for item: {item_id}")

        conn = get_db_connection()

        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=365 * years_back)

            offset = 0
            count = 500
            item_total = 0

            while True:
                txn_request = TransactionsGetRequest(
                    access_token=access_token,
                    start_date=start_date,
                    end_date=end_date,
                    options={
                        'count': count,
                        'offset': offset
                    }
                )
                response = plaid_client.transactions_get(txn_request).to_dict()
                transactions = response['transactions']
                total_transactions = response['total_transactions']

                if not transactions:
                    break

                for txn in transactions:
                    save_transaction(conn, txn)
                    item_total += 1

                print(f"[{datetime.now().isoformat()}] Fetched {len(transactions)} transactions (offset: {offset}, total: {total_transactions})")

                offset += len(transactions)

                if offset >= total_transactions:
                    break

                time.sleep(0.5)

            total_fetched += item_total
            print(f"[{datetime.now().isoformat()}] Item {item_id}: Fetched {item_total} historical transactions")

        except plaid.ApiException as e:
            print(f"[{datetime.now().isoformat()}] ERROR: Plaid API error for item {item_id}: {e}")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ERROR: {e}")
        finally:
            conn.close()

    print(f"[{datetime.now().isoformat()}] Historical fetch complete. Total: {total_fetched} transactions")

    return {'transactions_fetched': total_fetched}


if __name__ == '__main__':
    result = fetch_historical()
    print(json.dumps(result, indent=2))
