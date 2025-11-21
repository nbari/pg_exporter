#!/bin/bash
# Generate self-signed SSL certificates for PostgreSQL testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/ssl-certs"

echo "🔐 Generating SSL certificates for PostgreSQL testing..."

# Create directory
mkdir -p "${CERT_DIR}"

# Generate private key
echo "  📝 Generating private key..."
openssl genrsa -out "${CERT_DIR}/server.key" 2048

# Generate certificate signing request and self-signed certificate
echo "  📜 Generating certificate..."
openssl req -new -x509 \
  -key "${CERT_DIR}/server.key" \
  -out "${CERT_DIR}/server.crt" \
  -days 365 \
  -subj "/C=US/ST=Test/L=Test/O=pg_exporter-test/CN=localhost"

# Set permissions
echo "  🔒 Setting permissions..."
chmod 600 "${CERT_DIR}/server.key"
chmod 644 "${CERT_DIR}/server.crt"

# Generate certificate info
openssl x509 -in "${CERT_DIR}/server.crt" -text -noout > "${CERT_DIR}/cert-info.txt"

echo "✅ SSL certificates generated successfully!"
echo ""
echo "Certificate files:"
echo "  📁 Directory:  ${CERT_DIR}"
echo "  🔑 Private key: server.key"
echo "  📜 Certificate: server.crt"
echo "  ℹ️  Info:       cert-info.txt"
echo ""
echo "Certificate details:"
openssl x509 -in "${CERT_DIR}/server.crt" -noout -dates -subject -issuer
echo ""
echo "To use with Docker:"
echo "  docker-compose -f tests/docker-compose-ssl.yml up -d"
echo ""
echo "To test connection:"
echo "  psql 'postgresql://postgres:postgres@localhost:5433/postgres?sslmode=require'"
