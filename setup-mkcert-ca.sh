#!/bin/bash
set -e

echo "🔹 Appending mkcert root CA to Python certifi bundle..."

# Locate mkcert root CA
MKCERT_ROOT_CA="$(mkcert -CAROOT)/rootCA.pem"

if [[ ! -f "$MKCERT_ROOT_CA" ]]; then
    echo "mkcert root CA not found at $MKCERT_ROOT_CA"
    exit 1
fi

# Find Python certifi CA bundle
CERTIFI_CA=$(python3 -m certifi)

if [[ ! -f "$CERTIFI_CA" ]]; then
    echo "Could not find certifi CA bundle"
    exit 1
fi

echo " mkcert root CA: $MKCERT_ROOT_CA"
echo " Python certifi bundle: $CERTIFI_CA"

# Check if mkcert root CA is already in the bundle
if grep -q "BEGIN CERTIFICATE" "$CERTIFI_CA" | grep -q "$(openssl x509 -noout -fingerprint -in "$MKCERT_ROOT_CA")"; then
    echo "mkcert root CA already present in certifi bundle"
else
    # Append mkcert root CA to certifi bundle
    cat "$MKCERT_ROOT_CA" >> "$CERTIFI_CA"
    echo "mkcert root CA appended to Python certifi bundle"
fi

echo "Python will now trust mkcert certificates for all HTTPS requests."
