#!/bin/bash
# Setup script for deploying Plaid server to VPS
# Run this on your VPS as root

set -e

echo "=== Plaid Server VPS Setup ==="

# Install dependencies
apt update
apt install -y python3 python3-venv python3-pip nginx certbot python3-certbot-nginx

# Create application directory
mkdir -p /opt/plaid
cd /opt/plaid

echo "=== Copy your files to /opt/plaid ==="
echo "Run this from your local machine:"
echo "  scp -r quickstart/python/* root@your-vps:/opt/plaid/"
echo ""
read -p "Press Enter after copying files..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install --upgrade pip
pip install flask plaid-python python-dotenv psycopg2-binary

# Set permissions
chmod +x /opt/plaid/*.py

# Setup systemd service
cp /opt/plaid/deploy/plaid-server.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable plaid-server
systemctl start plaid-server

# Setup nginx
cp /opt/plaid/deploy/nginx-plaid.conf /etc/nginx/sites-available/plaid
ln -sf /etc/nginx/sites-available/plaid /etc/nginx/sites-enabled/

# Get SSL certificate
echo ""
echo "=== Getting SSL Certificate ==="
certbot --nginx -d plaid.benrishty.com

# Test and reload nginx
nginx -t
systemctl reload nginx

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Plaid server running at: https://plaid.benrishty.com"
echo "Webhook URL: https://plaid.benrishty.com/api/webhook"
echo ""
echo "Check status: systemctl status plaid-server"
echo "View logs: journalctl -u plaid-server -f"
echo ""
echo "Configure this webhook URL in your Plaid Dashboard!"
