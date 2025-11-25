#!/bin/bash
# Plaid Transaction Sync Script
# Run this on a schedule (e.g., hourly via cron)
#
# Cron examples:
#   Every hour:     0 * * * * /path/to/sync_transactions.sh
#   Every 6 hours:  0 */6 * * * /path/to/sync_transactions.sh
#   Daily at 6am:   0 6 * * * /path/to/sync_transactions.sh

cd /Users/benrishty/Desktop/Github/plaid/quickstart/python
source venv/bin/activate
python etl/sync_transactions.py
