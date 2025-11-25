#!/usr/bin/env python3
"""
Plaid ETL Scripts - Standalone scripts for scheduled data syncing

Usage:
    python etl.py sync_all           # Run all sync jobs
    python etl.py sync_transactions  # Sync new/modified transactions (incremental)
    python etl.py sync_balances      # Sync balances only
    python etl.py sync_accounts      # Sync accounts only
    python etl.py fetch_historical   # Fetch ALL historical transactions (up to 5 years)

Schedule with cron:
    # Sync transactions every hour
    0 * * * * cd /path/to/quickstart/python && ./venv/bin/python etl.py sync_transactions

    # Sync balances daily at 6am
    0 6 * * * cd /path/to/quickstart/python && ./venv/bin/python etl.py sync_balances
"""

import os
import sys
import json
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import plaid
from plaid.api import plaid_api
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from datetime import date, timedelta

# Load environment variables
load_dotenv()

# Configuration
PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'sandbox')
PLAID_COUNTRY_CODES = os.getenv('PLAID_COUNTRY_CODES', 'US').split(',')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

# Email settings for notifications
EMAIL_ENABLED = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
SMTP_HOST = os.getenv('SMTP_HOST')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
FROM_EMAIL = os.getenv('FROM_EMAIL')
TO_EMAILS = os.getenv('TO_EMAILS', '').split(',')

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


class ETLLogger:
    """Simple logger for ETL jobs"""
    def __init__(self, job_name):
        self.job_name = job_name
        self.start_time = datetime.now()
        self.logs = []

    def log(self, message, level='INFO'):
        timestamp = datetime.now().isoformat()
        log_entry = f"[{timestamp}] [{level}] [{self.job_name}] {message}"
        self.logs.append(log_entry)
        print(log_entry)

    def error(self, message):
        self.log(message, 'ERROR')

    def get_summary(self):
        duration = (datetime.now() - self.start_time).total_seconds()
        return {
            'job_name': self.job_name,
            'start_time': self.start_time.isoformat(),
            'duration_seconds': duration,
            'logs': self.logs
        }


def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )


def get_all_items():
    """Get all Plaid items from database"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT item_id, access_token FROM plaid_items')
    items = cur.fetchall()
    cur.close()
    conn.close()
    return items


def get_sync_cursor(conn, item_id):
    """Get sync cursor for an item"""
    cur = conn.cursor()
    cur.execute('SELECT cursor FROM sync_cursors WHERE item_id = %s', (item_id,))
    result = cur.fetchone()
    cur.close()
    return result['cursor'] if result else ''


def save_sync_cursor(conn, item_id, cursor):
    """Save sync cursor for an item"""
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


def save_account(conn, account_data, item_id):
    """Save account data to database"""
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


def save_balance_history(conn, account_id, balances):
    """Save balance snapshot to history table"""
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO account_balance_history (
            account_id, current_balance, available_balance, limit_amount, iso_currency_code
        ) VALUES (%s, %s, %s, %s, %s)
    ''', (
        account_id,
        balances.get('current'),
        balances.get('available'),
        balances.get('limit'),
        balances.get('iso_currency_code')
    ))
    conn.commit()
    cur.close()


def save_transaction(conn, txn_data):
    """Save transaction data to database"""
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
    """Delete a transaction from the database"""
    cur = conn.cursor()
    cur.execute('DELETE FROM financial_transactions WHERE transaction_id = %s', (transaction_id,))
    conn.commit()
    cur.close()


def send_notification(subject, body):
    """Send email notification if enabled"""
    if not EMAIL_ENABLED:
        return

    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = ', '.join(TO_EMAILS)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"Failed to send notification: {e}")


# ============================================
# ETL Jobs
# ============================================

def sync_transactions():
    """Sync transactions for all items"""
    logger = ETLLogger('sync_transactions')
    logger.log("Starting transaction sync")

    items = get_all_items()
    logger.log(f"Found {len(items)} items to sync")

    total_added = 0
    total_modified = 0
    total_removed = 0

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        logger.log(f"Syncing transactions for item: {item_id}")

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

            # Save to database
            for txn in added:
                save_transaction(conn, txn)
            for txn in modified:
                save_transaction(conn, txn)
            for txn in removed:
                delete_transaction(conn, txn['transaction_id'])

            # Save cursor
            if cursor:
                save_sync_cursor(conn, item_id, cursor)

            total_added += len(added)
            total_modified += len(modified)
            total_removed += len(removed)

            logger.log(f"Item {item_id}: +{len(added)} added, ~{len(modified)} modified, -{len(removed)} removed")

        except plaid.ApiException as e:
            logger.error(f"Plaid API error for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Error syncing item {item_id}: {e}")
        finally:
            conn.close()

    logger.log(f"Transaction sync complete. Total: +{total_added} added, ~{total_modified} modified, -{total_removed} removed")

    # Send notification
    if total_added > 0 or total_modified > 0 or total_removed > 0:
        send_notification(
            "Plaid Transaction Sync Complete",
            f"Transaction sync completed.\n\nAdded: {total_added}\nModified: {total_modified}\nRemoved: {total_removed}"
        )

    return logger.get_summary()


def sync_balances():
    """Sync account balances for all items"""
    logger = ETLLogger('sync_balances')
    logger.log("Starting balance sync")

    items = get_all_items()
    logger.log(f"Found {len(items)} items to sync")

    total_accounts = 0

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        logger.log(f"Syncing balances for item: {item_id}")

        conn = get_db_connection()

        try:
            balance_request = AccountsBalanceGetRequest(access_token=access_token)
            response = plaid_client.accounts_balance_get(balance_request).to_dict()

            for account in response['accounts']:
                save_account(conn, account, item_id)
                save_balance_history(conn, account['account_id'], account.get('balances', {}))
                total_accounts += 1

            logger.log(f"Item {item_id}: Updated {len(response['accounts'])} accounts")

        except plaid.ApiException as e:
            logger.error(f"Plaid API error for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Error syncing item {item_id}: {e}")
        finally:
            conn.close()

    logger.log(f"Balance sync complete. Updated {total_accounts} accounts")

    return logger.get_summary()


def sync_accounts():
    """Sync account information for all items"""
    logger = ETLLogger('sync_accounts')
    logger.log("Starting account sync")

    items = get_all_items()
    logger.log(f"Found {len(items)} items to sync")

    total_accounts = 0

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        logger.log(f"Syncing accounts for item: {item_id}")

        conn = get_db_connection()

        try:
            accounts_request = AccountsGetRequest(access_token=access_token)
            response = plaid_client.accounts_get(accounts_request).to_dict()

            for account in response['accounts']:
                save_account(conn, account, item_id)
                total_accounts += 1

            logger.log(f"Item {item_id}: Synced {len(response['accounts'])} accounts")

        except plaid.ApiException as e:
            logger.error(f"Plaid API error for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Error syncing item {item_id}: {e}")
        finally:
            conn.close()

    logger.log(f"Account sync complete. Synced {total_accounts} accounts")

    return logger.get_summary()


def fetch_historical_transactions(years_back=5):
    """Fetch all historical transactions going back N years using TransactionsGet API"""
    logger = ETLLogger('fetch_historical_transactions')
    logger.log(f"Starting historical transaction fetch (going back {years_back} years)")

    items = get_all_items()
    logger.log(f"Found {len(items)} items to fetch")

    total_fetched = 0

    for item in items:
        item_id = item['item_id']
        access_token = item['access_token']
        logger.log(f"Fetching historical transactions for item: {item_id}")

        conn = get_db_connection()

        try:
            # Calculate date range - go back N years
            end_date = date.today()
            start_date = end_date - timedelta(days=365 * years_back)

            offset = 0
            count = 500  # Max per request
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

                # Save each transaction
                for txn in transactions:
                    save_transaction(conn, txn)
                    item_total += 1

                logger.log(f"Fetched {len(transactions)} transactions (offset: {offset}, total available: {total_transactions})")

                offset += len(transactions)

                # Check if we've fetched all available transactions
                if offset >= total_transactions:
                    break

                # Small delay to avoid rate limiting
                time.sleep(0.5)

            total_fetched += item_total
            logger.log(f"Item {item_id}: Fetched {item_total} historical transactions")

        except plaid.ApiException as e:
            logger.error(f"Plaid API error for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Error fetching item {item_id}: {e}")
        finally:
            conn.close()

    logger.log(f"Historical fetch complete. Total: {total_fetched} transactions")

    send_notification(
        "Plaid Historical Transaction Fetch Complete",
        f"Fetched {total_fetched} historical transactions going back {years_back} years."
    )

    return logger.get_summary()


def sync_all():
    """Run all sync jobs"""
    logger = ETLLogger('sync_all')
    logger.log("Starting full sync")

    results = {
        'accounts': sync_accounts(),
        'balances': sync_balances(),
        'transactions': sync_transactions()
    }

    logger.log("Full sync complete")

    # Send summary notification
    send_notification(
        "Plaid Full Sync Complete",
        f"Full sync completed at {datetime.now().isoformat()}\n\n"
        f"Check logs for details."
    )

    return results


# ============================================
# CLI Entry Point
# ============================================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    commands = {
        'sync_all': sync_all,
        'sync_transactions': sync_transactions,
        'sync_balances': sync_balances,
        'sync_accounts': sync_accounts,
        'fetch_historical': fetch_historical_transactions,
    }

    if command not in commands:
        print(f"Unknown command: {command}")
        print(f"Available commands: {', '.join(commands.keys())}")
        sys.exit(1)

    try:
        result = commands[command]()
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error running {command}: {e}")
        send_notification(f"Plaid ETL Error: {command}", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
