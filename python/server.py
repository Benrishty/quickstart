# Read env vars from .env file
import base64
import os
import datetime as dt
import json
import time
from datetime import date, timedelta
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv
from flask import Flask, request, jsonify, g
import plaid
from plaid.model.payment_amount import PaymentAmount
from plaid.model.payment_amount_currency import PaymentAmountCurrency
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.recipient_bacs_nullable import RecipientBACSNullable
from plaid.model.payment_initiation_address import PaymentInitiationAddress
from plaid.model.payment_initiation_recipient_create_request import PaymentInitiationRecipientCreateRequest
from plaid.model.payment_initiation_payment_create_request import PaymentInitiationPaymentCreateRequest
from plaid.model.payment_initiation_payment_get_request import PaymentInitiationPaymentGetRequest
from plaid.model.link_token_create_request_payment_initiation import LinkTokenCreateRequestPaymentInitiation
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.user_create_request import UserCreateRequest
from plaid.model.consumer_report_user_identity import ConsumerReportUserIdentity
from plaid.model.asset_report_create_request import AssetReportCreateRequest
from plaid.model.asset_report_create_request_options import AssetReportCreateRequestOptions
from plaid.model.asset_report_user import AssetReportUser
from plaid.model.asset_report_get_request import AssetReportGetRequest
from plaid.model.asset_report_pdf_get_request import AssetReportPDFGetRequest
from plaid.model.auth_get_request import AuthGetRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.identity_get_request import IdentityGetRequest
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.transfer_authorization_create_request import TransferAuthorizationCreateRequest
from plaid.model.transfer_create_request import TransferCreateRequest
from plaid.model.transfer_get_request import TransferGetRequest
from plaid.model.transfer_network import TransferNetwork
from plaid.model.transfer_type import TransferType
from plaid.model.transfer_authorization_user_in_request import TransferAuthorizationUserInRequest
from plaid.model.ach_class import ACHClass
from plaid.model.transfer_create_idempotency_key import TransferCreateIdempotencyKey
from plaid.model.transfer_user_address_in_request import TransferUserAddressInRequest
from plaid.model.signal_evaluate_request import SignalEvaluateRequest
from plaid.model.statements_list_request import StatementsListRequest
from plaid.model.link_token_create_request_statements import LinkTokenCreateRequestStatements
from plaid.model.link_token_create_request_cra_options import LinkTokenCreateRequestCraOptions
from plaid.model.statements_download_request import StatementsDownloadRequest
from plaid.model.consumer_report_permissible_purpose import ConsumerReportPermissiblePurpose
from plaid.model.cra_check_report_base_report_get_request import CraCheckReportBaseReportGetRequest
from plaid.model.cra_check_report_pdf_get_request import CraCheckReportPDFGetRequest
from plaid.model.cra_check_report_income_insights_get_request import CraCheckReportIncomeInsightsGetRequest
from plaid.model.cra_check_report_partner_insights_get_request import CraCheckReportPartnerInsightsGetRequest
from plaid.model.cra_pdf_add_ons import CraPDFAddOns
from plaid.api import plaid_api

load_dotenv()


app = Flask(__name__)

PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV', 'sandbox')
PLAID_PRODUCTS = os.getenv('PLAID_PRODUCTS', 'transactions').split(',')
PLAID_COUNTRY_CODES = os.getenv('PLAID_COUNTRY_CODES', 'US').split(',')
SIGNAL_RULESET_KEY = os.getenv('SIGNAL_RULESET_KEY', '')

# PostgreSQL configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            cursor_factory=RealDictCursor
        )
    return g.db

def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()

    # Institutions table - stores bank/financial institution info
    cur.execute('''
        CREATE TABLE IF NOT EXISTS institutions (
            id SERIAL PRIMARY KEY,
            institution_id VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            url TEXT,
            logo TEXT,
            primary_color VARCHAR(50),
            country_codes TEXT[],
            products TEXT[],
            routing_numbers TEXT[],
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Plaid Items table - represents a connection to a financial institution
    cur.execute('''
        CREATE TABLE IF NOT EXISTS plaid_items (
            id SERIAL PRIMARY KEY,
            item_id VARCHAR(255) UNIQUE NOT NULL,
            access_token TEXT NOT NULL,
            institution_id VARCHAR(255) REFERENCES institutions(institution_id),
            user_token TEXT,
            payment_id VARCHAR(255),
            transfer_id VARCHAR(255),
            consent_expiration_time TIMESTAMP,
            update_type VARCHAR(50),
            webhook TEXT,
            error JSONB,
            available_products TEXT[],
            billed_products TEXT[],
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Financial Accounts table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial_accounts (
            id SERIAL PRIMARY KEY,
            account_id VARCHAR(255) UNIQUE NOT NULL,
            item_id VARCHAR(255) REFERENCES plaid_items(item_id),
            name VARCHAR(255),
            official_name VARCHAR(255),
            type VARCHAR(50),
            subtype VARCHAR(50),
            mask VARCHAR(20),
            current_balance DECIMAL(15, 2),
            available_balance DECIMAL(15, 2),
            limit_amount DECIMAL(15, 2),
            iso_currency_code VARCHAR(10),
            unofficial_currency_code VARCHAR(10),
            persistent_account_id VARCHAR(255),
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Transaction Categories table (Plaid's category hierarchy)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS transaction_categories (
            id SERIAL PRIMARY KEY,
            category_id VARCHAR(255) UNIQUE NOT NULL,
            primary_category VARCHAR(255),
            detailed_category VARCHAR(255),
            confidence_level VARCHAR(50),
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Financial Transactions table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS financial_transactions (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(255) UNIQUE NOT NULL,
            account_id VARCHAR(255) REFERENCES financial_accounts(account_id),
            amount DECIMAL(15, 2),
            iso_currency_code VARCHAR(10),
            unofficial_currency_code VARCHAR(10),
            date DATE,
            datetime TIMESTAMP,
            authorized_date DATE,
            authorized_datetime TIMESTAMP,
            name VARCHAR(500),
            merchant_name VARCHAR(255),
            merchant_entity_id VARCHAR(255),
            logo_url TEXT,
            website TEXT,
            payment_channel VARCHAR(50),
            pending BOOLEAN,
            pending_transaction_id VARCHAR(255),
            account_owner VARCHAR(255),
            transaction_code VARCHAR(50),
            transaction_type VARCHAR(50),
            category_id VARCHAR(255),
            personal_finance_category_primary VARCHAR(255),
            personal_finance_category_detailed VARCHAR(255),
            personal_finance_category_confidence VARCHAR(50),
            personal_finance_category_icon_url TEXT,
            location_address VARCHAR(255),
            location_city VARCHAR(100),
            location_region VARCHAR(100),
            location_postal_code VARCHAR(20),
            location_country VARCHAR(100),
            location_lat DECIMAL(10, 7),
            location_lon DECIMAL(10, 7),
            location_store_number VARCHAR(50),
            payment_meta_reference_number VARCHAR(255),
            payment_meta_ppd_id VARCHAR(255),
            payment_meta_payee VARCHAR(255),
            payment_meta_by_order_of VARCHAR(255),
            payment_meta_payer VARCHAR(255),
            payment_meta_payment_method VARCHAR(50),
            payment_meta_payment_processor VARCHAR(255),
            payment_meta_reason VARCHAR(255),
            counterparties JSONB,
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Account Balances History table (track balance changes over time)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS account_balance_history (
            id SERIAL PRIMARY KEY,
            account_id VARCHAR(255) REFERENCES financial_accounts(account_id),
            current_balance DECIMAL(15, 2),
            available_balance DECIMAL(15, 2),
            limit_amount DECIMAL(15, 2),
            iso_currency_code VARCHAR(10),
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Sync Cursors table (for transaction sync)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sync_cursors (
            id SERIAL PRIMARY KEY,
            item_id VARCHAR(255) REFERENCES plaid_items(item_id) UNIQUE,
            cursor TEXT,
            last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create indexes for better query performance
    cur.execute('CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON financial_transactions(account_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON financial_transactions(date)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_transactions_merchant ON financial_transactions(merchant_name)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_transactions_category ON financial_transactions(personal_finance_category_primary)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_accounts_item_id ON financial_accounts(item_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_balance_history_account ON account_balance_history(account_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_balance_history_date ON account_balance_history(recorded_at)')

    conn.commit()
    cur.close()
    conn.close()

def empty_to_none(field):
    value = os.getenv(field)
    if value is None or len(value) == 0:
        return None
    return value

host = plaid.Environment.Sandbox

if PLAID_ENV == 'sandbox':
    host = plaid.Environment.Sandbox

if PLAID_ENV == 'production':
    host = plaid.Environment.Production

# Parameters used for the OAuth redirect Link flow.
#
# Set PLAID_REDIRECT_URI to 'http://localhost:3000/'
# The OAuth redirect flow requires an endpoint on the developer's website
# that the bank website should redirect to. You will need to configure
# this redirect URI for your client ID through the Plaid developer dashboard
# at https://dashboard.plaid.com/team/api.
PLAID_REDIRECT_URI = empty_to_none('PLAID_REDIRECT_URI')

configuration = plaid.Configuration(
    host=host,
    api_key={
        'clientId': PLAID_CLIENT_ID,
        'secret': PLAID_SECRET,
        'plaidVersion': '2020-09-14'
    }
)

api_client = plaid.ApiClient(configuration)
client = plaid_api.PlaidApi(api_client)

products = []
for product in PLAID_PRODUCTS:
    products.append(Products(product))


# Initialize database and register teardown
app.teardown_appcontext(close_db)

# Initialize database tables on startup
try:
    init_db()
    print("Database initialized successfully")
except Exception as e:
    print(f"Warning: Could not initialize database: {e}")

# Helper functions to get/set tokens from database
def get_current_item():
    """Get the most recent item from database"""
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT * FROM plaid_items ORDER BY created_at DESC LIMIT 1')
    item = cur.fetchone()
    cur.close()
    return item

def save_item(item_id, access_token, user_token=None, payment_id=None, transfer_id=None):
    """Save or update an item in the database"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        INSERT INTO plaid_items (item_id, access_token, user_token, payment_id, transfer_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (item_id) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            user_token = COALESCE(EXCLUDED.user_token, plaid_items.user_token),
            payment_id = COALESCE(EXCLUDED.payment_id, plaid_items.payment_id),
            transfer_id = COALESCE(EXCLUDED.transfer_id, plaid_items.transfer_id),
            updated_at = CURRENT_TIMESTAMP
    ''', (item_id, access_token, user_token, payment_id, transfer_id))
    db.commit()
    cur.close()

def get_access_token_from_db():
    """Get access token from the most recent item"""
    item = get_current_item()
    return item['access_token'] if item else None

def get_item_id_from_db():
    """Get item_id from the most recent item"""
    item = get_current_item()
    return item['item_id'] if item else None

# ============================================
# Database helper functions for Plaid data
# ============================================

def save_institution(institution_data):
    """Save institution data to database"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        INSERT INTO institutions (
            institution_id, name, url, logo, primary_color,
            country_codes, products, routing_numbers, raw_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (institution_id) DO UPDATE SET
            name = EXCLUDED.name,
            url = EXCLUDED.url,
            logo = EXCLUDED.logo,
            primary_color = EXCLUDED.primary_color,
            country_codes = EXCLUDED.country_codes,
            products = EXCLUDED.products,
            routing_numbers = EXCLUDED.routing_numbers,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
    ''', (
        institution_data.get('institution_id'),
        institution_data.get('name'),
        institution_data.get('url'),
        institution_data.get('logo'),
        institution_data.get('primary_color'),
        institution_data.get('country_codes'),
        institution_data.get('products'),
        institution_data.get('routing_numbers'),
        json.dumps(institution_data, default=str)
    ))
    db.commit()
    cur.close()

def save_item_full(item_data, access_token, institution_id=None):
    """Save full item data to database"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        INSERT INTO plaid_items (
            item_id, access_token, institution_id, consent_expiration_time,
            update_type, webhook, error, available_products, billed_products, raw_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (item_id) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            institution_id = COALESCE(EXCLUDED.institution_id, plaid_items.institution_id),
            consent_expiration_time = EXCLUDED.consent_expiration_time,
            update_type = EXCLUDED.update_type,
            webhook = EXCLUDED.webhook,
            error = EXCLUDED.error,
            available_products = EXCLUDED.available_products,
            billed_products = EXCLUDED.billed_products,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
    ''', (
        item_data.get('item_id'),
        access_token,
        institution_id or item_data.get('institution_id'),
        item_data.get('consent_expiration_time'),
        item_data.get('update_type'),
        item_data.get('webhook'),
        json.dumps(item_data.get('error'), default=str) if item_data.get('error') else None,
        item_data.get('available_products'),
        item_data.get('billed_products'),
        json.dumps(item_data, default=str)
    ))
    db.commit()
    cur.close()

def save_account(account_data, item_id):
    """Save account data to database"""
    db = get_db()
    cur = db.cursor()

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
    db.commit()
    cur.close()

def save_account_balance_history(account_id, balances):
    """Save balance snapshot to history table"""
    db = get_db()
    cur = db.cursor()
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
    db.commit()
    cur.close()

def save_transaction(txn_data):
    """Save transaction data to database"""
    db = get_db()
    cur = db.cursor()

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
    db.commit()
    cur.close()

def delete_transaction(transaction_id):
    """Delete a transaction from the database"""
    db = get_db()
    cur = db.cursor()
    cur.execute('DELETE FROM financial_transactions WHERE transaction_id = %s', (transaction_id,))
    db.commit()
    cur.close()

def save_sync_cursor(item_id, cursor):
    """Save or update sync cursor for an item"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        INSERT INTO sync_cursors (item_id, cursor, last_synced_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (item_id) DO UPDATE SET
            cursor = EXCLUDED.cursor,
            last_synced_at = CURRENT_TIMESTAMP
    ''', (item_id, cursor))
    db.commit()
    cur.close()

def get_sync_cursor(item_id):
    """Get sync cursor for an item"""
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT cursor FROM sync_cursors WHERE item_id = %s', (item_id,))
    result = cur.fetchone()
    cur.close()
    return result['cursor'] if result else ''


@app.route('/api/info', methods=['POST'])
def info():
    item = get_current_item()
    return jsonify({
        'item_id': item['item_id'] if item else None,
        'access_token': item['access_token'] if item else None,
        'products': PLAID_PRODUCTS
    })


@app.route('/api/create_link_token_for_payment', methods=['POST'])
def create_link_token_for_payment():
    global payment_id
    try:
        request = PaymentInitiationRecipientCreateRequest(
            name='John Doe',
            bacs=RecipientBACSNullable(account='26207729', sort_code='560029'),
            address=PaymentInitiationAddress(
                street=['street name 999'],
                city='city',
                postal_code='99999',
                country='GB'
            )
        )
        response = client.payment_initiation_recipient_create(
            request)
        recipient_id = response['recipient_id']

        request = PaymentInitiationPaymentCreateRequest(
            recipient_id=recipient_id,
            reference='TestPayment',
            amount=PaymentAmount(
                PaymentAmountCurrency('GBP'),
                value=100.00
            )
        )
        response = client.payment_initiation_payment_create(
            request
        )
        pretty_print_response(response.to_dict())
        
        # We store the payment_id in memory for demo purposes - in production, store it in a secure
        # persistent data store along with the Payment metadata, such as userId.
        payment_id = response['payment_id']
        
        linkRequest = LinkTokenCreateRequest(
            # The 'payment_initiation' product has to be the only element in the 'products' list.
            products=[Products('payment_initiation')],
            client_name='Plaid Test',
            # Institutions from all listed countries will be shown.
            country_codes=list(map(lambda x: CountryCode(x), PLAID_COUNTRY_CODES)),
            language='en',
            user=LinkTokenCreateRequestUser(
                # This should correspond to a unique id for the current user.
                # Typically, this will be a user ID number from your application.
                # Personally identifiable information, such as an email address or phone number, should not be used here.
                client_user_id=str(time.time())
            ),
            payment_initiation=LinkTokenCreateRequestPaymentInitiation(
                payment_id=payment_id
            )
        )

        if PLAID_REDIRECT_URI!=None:
            linkRequest['redirect_uri']=PLAID_REDIRECT_URI
        linkResponse = client.link_token_create(linkRequest)
        pretty_print_response(linkResponse.to_dict())
        return jsonify(linkResponse.to_dict())
    except plaid.ApiException as e:
        return json.loads(e.body)


@app.route('/api/create_link_token', methods=['POST'])
def create_link_token():
    global user_token
    try:
        request = LinkTokenCreateRequest(
            products=products,
            client_name="Plaid Quickstart",
            country_codes=list(map(lambda x: CountryCode(x), PLAID_COUNTRY_CODES)),
            language='en',
            user=LinkTokenCreateRequestUser(
                client_user_id=str(time.time())
            )
        )
        if PLAID_REDIRECT_URI!=None:
            request['redirect_uri']=PLAID_REDIRECT_URI
        if Products('statements') in products:
            statements=LinkTokenCreateRequestStatements(
                end_date=date.today(),
                start_date=date.today()-timedelta(days=30)
            )
            request['statements']=statements

        cra_products = ["cra_base_report", "cra_income_insights", "cra_partner_insights"]
        if any(product in cra_products for product in PLAID_PRODUCTS):
            request['user_token'] = user_token
            request['consumer_report_permissible_purpose'] = ConsumerReportPermissiblePurpose('ACCOUNT_REVIEW_CREDIT')
            request['cra_options'] = LinkTokenCreateRequestCraOptions(
                days_requested=60
            )
    # create link token
        response = client.link_token_create(request)
        return jsonify(response.to_dict())
    except plaid.ApiException as e:
        print(e)
        return json.loads(e.body)

# Create a user token which can be used for Plaid Check, Income, or Multi-Item link flows
# https://plaid.com/docs/api/users/#usercreate
@app.route('/api/create_user_token', methods=['POST'])
def create_user_token():
    global user_token
    try:
        consumer_report_user_identity = None
        user_create_request = UserCreateRequest(
            # Typically this will be a user ID number from your application. 
            client_user_id="user_" + str(uuid.uuid4())
        )

        cra_products = ["cra_base_report", "cra_income_insights", "cra_partner_insights"]
        if any(product in cra_products for product in PLAID_PRODUCTS):
            consumer_report_user_identity = ConsumerReportUserIdentity(
                first_name="Harry",
                last_name="Potter",
                date_of_birth= date(1980, 7, 31),
                phone_numbers= ['+16174567890'],
                emails= ['harrypotter@example.com'],
                primary_address= {
                    "city": 'New York',
                    "region": 'NY',
                    "street": '4 Privet Drive',
                    "postal_code": '11111',
                    "country": 'US'
                }
            )
            user_create_request["consumer_report_user_identity"] = consumer_report_user_identity

        user_response = client.user_create(user_create_request)
        user_token = user_response['user_token']
        return jsonify(user_response.to_dict())
    except plaid.ApiException as e:
        print(e)
        return jsonify(json.loads(e.body)), e.status


# Exchange token flow - exchange a Link public_token for
# an API access_token
# https://plaid.com/docs/#exchange-token-flow


@app.route('/api/set_access_token', methods=['POST'])
def set_access_token():
    public_token = request.form['public_token']
    try:
        exchange_request = ItemPublicTokenExchangeRequest(
            public_token=public_token)
        exchange_response = client.item_public_token_exchange(exchange_request)
        access_token = exchange_response['access_token']
        item_id = exchange_response['item_id']

        # Get item details and save to database
        item_request = ItemGetRequest(access_token=access_token)
        item_response = client.item_get(item_request)
        item_data = item_response.to_dict()['item']

        # Get and save institution data
        try:
            inst_request = InstitutionsGetByIdRequest(
                institution_id=item_data['institution_id'],
                country_codes=list(map(lambda x: CountryCode(x), PLAID_COUNTRY_CODES))
            )
            inst_response = client.institutions_get_by_id(inst_request)
            save_institution(inst_response.to_dict()['institution'])
        except Exception as e:
            print(f"Warning: Could not fetch institution: {e}")

        # Save item to database
        save_item_full(item_data, access_token)

        # Fetch and save accounts
        accounts_request = AccountsGetRequest(access_token=access_token)
        accounts_response = client.accounts_get(accounts_request)
        for account in accounts_response.to_dict()['accounts']:
            save_account(account, item_id)

        return jsonify(exchange_response.to_dict())
    except plaid.ApiException as e:
        return json.loads(e.body)


# Retrieve ACH or ETF account numbers for an Item
# https://plaid.com/docs/#auth


@app.route('/api/auth', methods=['GET'])
def get_auth():
    try:
       access_token = get_access_token_from_db()
       auth_request = AuthGetRequest(
            access_token=access_token
        )
       response = client.auth_get(auth_request)
       pretty_print_response(response.to_dict())
       return jsonify(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve Transactions for an Item
# https://plaid.com/docs/#transactions


@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    access_token = get_access_token_from_db()
    item_id = get_item_id_from_db()

    # Get stored cursor or start fresh
    cursor = get_sync_cursor(item_id) if item_id else ''

    # New transaction updates since "cursor"
    added = []
    modified = []
    removed = [] # Removed transaction ids
    has_more = True
    try:
        # Iterate through each page of new transaction updates for item
        while has_more:
            txn_request = TransactionsSyncRequest(
                access_token=access_token,
                cursor=cursor,
            )
            response = client.transactions_sync(txn_request).to_dict()
            cursor = response['next_cursor']
            # If no transactions are available yet, wait and poll the endpoint.
            # Normally, we would listen for a webhook, but the Quickstart doesn't
            # support webhooks. For a webhook example, see
            # https://github.com/plaid/tutorial-resources or
            # https://github.com/plaid/pattern
            if cursor == '':
                time.sleep(2)
                continue
            # If cursor is not an empty string, we got results,
            # so add this page of results
            added.extend(response['added'])
            modified.extend(response['modified'])
            removed.extend(response['removed'])
            has_more = response['has_more']
            pretty_print_response(response)

        # Save transactions to database
        for txn in added:
            save_transaction(txn)
        for txn in modified:
            save_transaction(txn)
        for txn in removed:
            delete_transaction(txn['transaction_id'])

        # Save the cursor for next sync
        if item_id and cursor:
            save_sync_cursor(item_id, cursor)

        # Return the 8 most recent transactions
        latest_transactions = sorted(added, key=lambda t: t['date'])[-8:]
        return jsonify({
            'latest_transactions': latest_transactions,
            'total_added': len(added),
            'total_modified': len(modified),
            'total_removed': len(removed)
        })

    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve Identity data for an Item
# https://plaid.com/docs/#identity


@app.route('/api/identity', methods=['GET'])
def get_identity():
    try:
        access_token = get_access_token_from_db()
        identity_request = IdentityGetRequest(
            access_token=access_token
        )
        response = client.identity_get(identity_request)
        pretty_print_response(response.to_dict())
        return jsonify(
            {'error': None, 'identity': response.to_dict()['accounts']})
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve real-time balance data for each of an Item's accounts
# https://plaid.com/docs/#balance


@app.route('/api/balance', methods=['GET'])
def get_balance():
    try:
        access_token = get_access_token_from_db()
        item_id = get_item_id_from_db()
        balance_request = AccountsBalanceGetRequest(access_token=access_token)
        balance_response = client.accounts_balance_get(balance_request)
        response_data = balance_response.to_dict()

        # Save updated account data and balance history
        for account in response_data['accounts']:
            save_account(account, item_id)
            save_account_balance_history(account['account_id'], account.get('balances', {}))

        pretty_print_response(response_data)
        return jsonify(response_data)
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve an Item's accounts
# https://plaid.com/docs/#accounts


@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    try:
        access_token = get_access_token_from_db()
        accounts_request = AccountsGetRequest(
            access_token=access_token
        )
        response = client.accounts_get(accounts_request)
        pretty_print_response(response.to_dict())
        return jsonify(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Create and then retrieve an Asset Report for one or more Items. Note that an
# Asset Report can contain up to 100 items, but for simplicity we're only
# including one Item here.
# https://plaid.com/docs/#assets


@app.route('/api/assets', methods=['GET'])
def get_assets():
    try:
        access_token = get_access_token_from_db()
        asset_request = AssetReportCreateRequest(
            access_tokens=[access_token],
            days_requested=60,
            options=AssetReportCreateRequestOptions(
                webhook='https://www.example.com',
                client_report_id='123',
                user=AssetReportUser(
                    client_user_id='789',
                    first_name='Jane',
                    middle_name='Leah',
                    last_name='Doe',
                    ssn='123-45-6789',
                    phone_number='(555) 123-4567',
                    email='jane.doe@example.com',
                )
            )
        )

        response = client.asset_report_create(asset_request)
        pretty_print_response(response.to_dict())
        asset_report_token = response['asset_report_token']

        # Poll for the completion of the Asset Report.
        request = AssetReportGetRequest(
            asset_report_token=asset_report_token,
        )
        response = poll_with_retries(lambda: client.asset_report_get(request))
        asset_report_json = response['report']

        request = AssetReportPDFGetRequest(
            asset_report_token=asset_report_token,
        )
        pdf = client.asset_report_pdf_get(request)
        return jsonify({
            'error': None,
            'json': asset_report_json.to_dict(),
            'pdf': base64.b64encode(pdf.read()).decode('utf-8'),
        })
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve investment holdings data for an Item
# https://plaid.com/docs/#investments


@app.route('/api/holdings', methods=['GET'])
def get_holdings():
    try:
        access_token = get_access_token_from_db()
        holdings_request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = client.investments_holdings_get(holdings_request)
        pretty_print_response(response.to_dict())
        return jsonify({'error': None, 'holdings': response.to_dict()})
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve Investment Transactions for an Item
# https://plaid.com/docs/#investments


@app.route('/api/investments_transactions', methods=['GET'])
def get_investments_transactions():
    # Pull transactions for the last 30 days
    access_token = get_access_token_from_db()
    start_date = (dt.datetime.now() - dt.timedelta(days=(30)))
    end_date = dt.datetime.now()
    try:
        options = InvestmentsTransactionsGetRequestOptions()
        inv_txn_request = InvestmentsTransactionsGetRequest(
            access_token=access_token,
            start_date=start_date.date(),
            end_date=end_date.date(),
            options=options
        )
        response = client.investments_transactions_get(
            inv_txn_request)
        pretty_print_response(response.to_dict())
        return jsonify(
            {'error': None, 'investments_transactions': response.to_dict()})

    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

# This functionality is only relevant for the ACH Transfer product.
# Authorize a transfer

@app.route('/api/transfer_authorize', methods=['GET'])
def transfer_authorization():
    global authorization_id
    global account_id
    access_token = get_access_token_from_db()
    acct_request = AccountsGetRequest(access_token=access_token)
    response = client.accounts_get(acct_request)
    account_id = response['accounts'][0]['account_id']
    try:
        transfer_auth_request = TransferAuthorizationCreateRequest(
            access_token=access_token,
            account_id=account_id,
            type=TransferType('debit'),
            network=TransferNetwork('ach'),
            amount='1.00',
            ach_class=ACHClass('ppd'),
            user=TransferAuthorizationUserInRequest(
                legal_name='FirstName LastName',
                email_address='foobar@email.com',
                address=TransferUserAddressInRequest(
                    street='123 Main St.',
                    city='San Francisco',
                    region='CA',
                    postal_code='94053',
                    country='US'
                ),
            ),
        )
        response = client.transfer_authorization_create(transfer_auth_request)
        pretty_print_response(response.to_dict())
        authorization_id = response['authorization']['id']
        return jsonify(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

# Create Transfer for a specified Transfer ID

@app.route('/api/transfer_create', methods=['GET'])
def transfer():
    try:
        access_token = get_access_token_from_db()
        transfer_create_request = TransferCreateRequest(
            access_token=access_token,
            account_id=account_id,
            authorization_id=authorization_id,
            description='Debit')
        response = client.transfer_create(transfer_create_request)
        pretty_print_response(response.to_dict())
        return jsonify(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

@app.route('/api/statements', methods=['GET'])
def statements():
    try:
        access_token = get_access_token_from_db()
        statements_request = StatementsListRequest(access_token=access_token)
        response = client.statements_list(statements_request)
        pretty_print_response(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)
    try:
        request = StatementsDownloadRequest(
            access_token=access_token,
            statement_id=response['accounts'][0]['statements'][0]['statement_id']
        )
        pdf = client.statements_download(request)
        return jsonify({
            'error': None,
            'json': response.to_dict(),
            'pdf': base64.b64encode(pdf.read()).decode('utf-8'),
        })
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)




@app.route('/api/signal_evaluate', methods=['GET'])
def signal():
    global account_id
    access_token = get_access_token_from_db()
    acct_request = AccountsGetRequest(access_token=access_token)
    response = client.accounts_get(acct_request)
    account_id = response['accounts'][0]['account_id']
    try:
        # Generate unique transaction ID using timestamp and random component
        client_transaction_id = f"txn-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"

        signal_request_params = {
            'access_token': access_token,
            'account_id': account_id,
            'client_transaction_id': client_transaction_id,
            'amount': 100.00
        }

        if SIGNAL_RULESET_KEY:
            signal_request_params['ruleset_key'] = SIGNAL_RULESET_KEY

        request = SignalEvaluateRequest(**signal_request_params)
        response = client.signal_evaluate(request)
        pretty_print_response(response.to_dict())
        return jsonify(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# This functionality is only relevant for the UK Payment Initiation product.
# Retrieve Payment for a specified Payment ID


@app.route('/api/payment', methods=['GET'])
def payment():
    global payment_id
    try:
        request = PaymentInitiationPaymentGetRequest(payment_id=payment_id)
        response = client.payment_initiation_payment_get(request)
        pretty_print_response(response.to_dict())
        return jsonify({'error': None, 'payment': response.to_dict()})
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)


# Retrieve high-level information about an Item
# https://plaid.com/docs/#retrieve-item


@app.route('/api/item', methods=['GET'])
def item():
    try:
        access_token = get_access_token_from_db()
        item_request = ItemGetRequest(access_token=access_token)
        response = client.item_get(item_request)
        inst_request = InstitutionsGetByIdRequest(
            institution_id=response['item']['institution_id'],
            country_codes=list(map(lambda x: CountryCode(x), PLAID_COUNTRY_CODES))
        )
        institution_response = client.institutions_get_by_id(inst_request)
        pretty_print_response(response.to_dict())
        pretty_print_response(institution_response.to_dict())
        return jsonify({'error': None, 'item': response.to_dict()[
            'item'], 'institution': institution_response.to_dict()['institution']})
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

# Retrieve CRA Base Report and PDF
# Base report: https://plaid.com/docs/check/api/#cracheck_reportbase_reportget
# PDF: https://plaid.com/docs/check/api/#cracheck_reportpdfget
@app.route('/api/cra/get_base_report', methods=['GET'])
def cra_check_report():
    try:
        get_response = poll_with_retries(lambda: client.cra_check_report_base_report_get(
            CraCheckReportBaseReportGetRequest(user_token=user_token, item_ids=[])
        ))
        pretty_print_response(get_response.to_dict())

        pdf_response = client.cra_check_report_pdf_get(
            CraCheckReportPDFGetRequest(user_token=user_token)
        )
        return jsonify({
            'report': get_response.to_dict()['report'],
            'pdf': base64.b64encode(pdf_response.read()).decode('utf-8')
        })
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

# Retrieve CRA Income Insights and PDF with Insights
# Income insights: https://plaid.com/docs/check/api/#cracheck_reportincome_insightsget
# PDF w/ income insights: https://plaid.com/docs/check/api/#cracheck_reportpdfget
@app.route('/api/cra/get_income_insights', methods=['GET'])
def cra_income_insights():
    try:
        get_response = poll_with_retries(lambda: client.cra_check_report_income_insights_get(
            CraCheckReportIncomeInsightsGetRequest(user_token=user_token))
        )
        pretty_print_response(get_response.to_dict())

        pdf_response = client.cra_check_report_pdf_get(
            CraCheckReportPDFGetRequest(user_token=user_token, add_ons=[CraPDFAddOns('cra_income_insights')]),
        )

        return jsonify({
            'report': get_response.to_dict()['report'],
            'pdf': base64.b64encode(pdf_response.read()).decode('utf-8')
        })
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

# Retrieve CRA Partner Insights
# https://plaid.com/docs/check/api/#cracheck_reportpartner_insightsget
@app.route('/api/cra/get_partner_insights', methods=['GET'])
def cra_partner_insights():
    try:
        response = poll_with_retries(lambda: client.cra_check_report_partner_insights_get(
            CraCheckReportPartnerInsightsGetRequest(user_token=user_token)
        ))
        pretty_print_response(response.to_dict())

        return jsonify(response.to_dict())
    except plaid.ApiException as e:
        error_response = format_error(e)
        return jsonify(error_response)

# Since this quickstart does not support webhooks, this function can be used to poll
# an API that would otherwise be triggered by a webhook.
# For a webhook example, see
# https://github.com/plaid/tutorial-resources or
# https://github.com/plaid/pattern
def poll_with_retries(request_callback, ms=1000, retries_left=20):
    while retries_left > 0:
        try:
            return request_callback()
        except plaid.ApiException as e:
            response = json.loads(e.body)
            if response['error_code'] != 'PRODUCT_NOT_READY':
                raise e
            elif retries_left == 0:
                raise Exception('Ran out of retries while polling') from e
            else:
                retries_left -= 1
                time.sleep(ms / 1000)


# ============================================
# Webhook Handler
# ============================================

def update_item_error(item_id, error_data):
    """Update item with error information"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        UPDATE plaid_items
        SET error = %s, updated_at = CURRENT_TIMESTAMP
        WHERE item_id = %s
    ''', (json.dumps(error_data, default=str), item_id))
    db.commit()
    cur.close()


def clear_item_error(item_id):
    """Clear error from item"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        UPDATE plaid_items
        SET error = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE item_id = %s
    ''', (item_id,))
    db.commit()
    cur.close()


@app.route('/api/webhook', methods=['POST'])
def webhook_handler():
    """
    Handle Plaid webhooks for automatic transaction syncing and error handling.

    Webhook types handled:
    - TRANSACTIONS: SYNC_UPDATES_AVAILABLE, DEFAULT_UPDATE, HISTORICAL_UPDATE
    - ITEM: ERROR, PENDING_EXPIRATION, USER_PERMISSION_REVOKED

    Configure webhook URL in Plaid Dashboard or when creating Link token.
    """
    data = request.get_json()
    webhook_type = data.get('webhook_type')
    webhook_code = data.get('webhook_code')
    item_id = data.get('item_id')

    print(f"[WEBHOOK] Received: {webhook_type} - {webhook_code} for item: {item_id}")

    # Transaction webhooks
    if webhook_type == 'TRANSACTIONS':
        if webhook_code in ['SYNC_UPDATES_AVAILABLE', 'DEFAULT_UPDATE', 'HISTORICAL_UPDATE']:
            # Trigger transaction sync for this item
            print(f"[WEBHOOK] Transaction updates available for item: {item_id}")
            try:
                # Get the access token for this item
                db = get_db()
                cur = db.cursor()
                cur.execute('SELECT access_token FROM plaid_items WHERE item_id = %s', (item_id,))
                result = cur.fetchone()
                cur.close()

                if result:
                    access_token = result['access_token']
                    cursor = get_sync_cursor(item_id)

                    # Sync transactions
                    added = []
                    modified = []
                    removed = []
                    has_more = True

                    while has_more:
                        txn_request = TransactionsSyncRequest(
                            access_token=access_token,
                            cursor=cursor,
                        )
                        response = client.transactions_sync(txn_request).to_dict()
                        cursor = response['next_cursor']

                        if cursor == '':
                            break

                        added.extend(response['added'])
                        modified.extend(response['modified'])
                        removed.extend(response['removed'])
                        has_more = response['has_more']

                    # Save to database
                    for txn in added:
                        save_transaction(txn)
                    for txn in modified:
                        save_transaction(txn)
                    for txn in removed:
                        delete_transaction(txn['transaction_id'])

                    if cursor:
                        save_sync_cursor(item_id, cursor)

                    print(f"[WEBHOOK] Synced: +{len(added)} added, ~{len(modified)} modified, -{len(removed)} removed")

            except Exception as e:
                print(f"[WEBHOOK] Error syncing transactions: {e}")

    # Item error webhooks
    elif webhook_type == 'ITEM':
        if webhook_code == 'ERROR':
            error = data.get('error', {})
            print(f"[WEBHOOK] Item error: {error.get('error_code')} - {error.get('error_message')}")
            update_item_error(item_id, error)

        elif webhook_code == 'PENDING_EXPIRATION':
            consent_expiration = data.get('consent_expiration_time')
            print(f"[WEBHOOK] Item pending expiration: {consent_expiration}")
            update_item_error(item_id, {
                'error_code': 'PENDING_EXPIRATION',
                'error_message': f'Consent expires at {consent_expiration}'
            })

        elif webhook_code == 'USER_PERMISSION_REVOKED':
            print(f"[WEBHOOK] User permission revoked for item: {item_id}")
            update_item_error(item_id, {
                'error_code': 'USER_PERMISSION_REVOKED',
                'error_message': 'User revoked permission for this item'
            })

    # Always return 200 to acknowledge receipt
    return jsonify({'status': 'received'}), 200


@app.route('/api/items/status', methods=['GET'])
def get_items_status():
    """Get status of all linked items, including any errors"""
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        SELECT pi.item_id, pi.institution_id, i.name as institution_name,
               pi.error, pi.created_at, pi.updated_at,
               sc.last_synced_at
        FROM plaid_items pi
        LEFT JOIN institutions i ON pi.institution_id = i.institution_id
        LEFT JOIN sync_cursors sc ON pi.item_id = sc.item_id
        ORDER BY pi.created_at DESC
    ''')
    items = cur.fetchall()
    cur.close()

    return jsonify({
        'items': [dict(item) for item in items],
        'total': len(items),
        'items_with_errors': len([i for i in items if i['error']])
    })


def pretty_print_response(response):
  print(json.dumps(response, indent=2, sort_keys=True, default=str))

def format_error(e):
    response = json.loads(e.body)
    return {'error': {'status_code': e.status, 'display_message':
                      response['error_message'], 'error_code': response['error_code'], 'error_type': response['error_type']}}

if __name__ == '__main__':
    app.run(port=int(os.getenv('PORT', 8000)))
