#!/bin/bash

# ──────────────────────────────────────────────
# MongoDB Data Import Script
# Reads connection details from config_file.ini
# ──────────────────────────────────────────────

CONFIG_FILE="$(dirname "$0")/../resources/config_file.ini"
DATA_FILE="$(dirname "$0")/../data/catalog.json"

# ──────────────────────────────────────────────
# Parse config.ini for [mongodb] section
# ──────────────────────────────────────────────
parse_config() {
    local key=$1
    grep -A 10 '^\[mongodb\]' "$CONFIG_FILE" | grep "^${key}" | head -1 | cut -d':' -f2- | tr -d ' '
}

HOST=$(parse_config "host")
PORT=$(parse_config "port")
USERNAME=$(parse_config "user")
PASSWORD=$(parse_config "password")
DATABASE=$(parse_config "database")
AUTH_SOURCE=$(parse_config "auth_source")
COLLECTION="catalog"

# ──────────────────────────────────────────────
# Validate
# ──────────────────────────────────────────────
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    exit 1
fi

if [ ! -f "$DATA_FILE" ]; then
    echo "Data file not found: $DATA_FILE"
    exit 1
fi

# ──────────────────────────────────────────────
# Build URI and Import
# ──────────────────────────────────────────────
ENCODED_PASSWORD=$(python3 -c "from urllib.parse import quote_plus; print(quote_plus('$PASSWORD'))")
MONGO_URI="mongodb://${USERNAME}:${ENCODED_PASSWORD}@${HOST}:${PORT}/${DATABASE}?authSource=${AUTH_SOURCE}"

echo "Starting data import into ${DATABASE}.${COLLECTION} from ${DATA_FILE}"
mongosh "$MONGO_URI" --eval "db.getSiblingDB('$DATABASE').$COLLECTION.drop()"

mongoimport --uri "$MONGO_URI" \
            --db "$DATABASE" \
            --collection "$COLLECTION" \
            --file "$DATA_FILE" \
            --mode=upsert \
            --jsonArray

if [ $? -eq 0 ]; then
    echo "Data imported successfully into ${DATABASE}.${COLLECTION}"
else
    echo "Error during data import. Check connection details, file format, and ensure mongo-tools are installed."
    exit 1
fi