#!/bin/bash

# ──────────────────────────────────────────────
# MySQL Data Import Script
# Reads connection details from config_file.ini
# ──────────────────────────────────────────────

CONFIG_FILE="$(dirname "$0")/../resources/config_file.ini"
DATA_FILE="$(dirname "$0")/../data/sales.csv"


# ──────────────────────────────────────────────
# Parse config.ini for [mysql] section
# ──────────────────────────────────────────────
parse_config() {
    local key=$1
    grep -A 10 '^\[mysql\]' "$CONFIG_FILE" | grep "^${key}" | head -1 | cut -d':' -f2- | tr -d ' '
}

HOST=$(parse_config "host")
PORT=$(parse_config "port")
USERNAME=$(parse_config "user")
PASSWORD=$(parse_config "password")
DATABASE=$(parse_config "database")
TABLE="sales_data"

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
# Copy CSV into container and load
# ──────────────────────────────────────────────
CONTAINER_NAME="mysql_db"
CONTAINER_PATH="/var/lib/mysql-files/oltpdata.csv"

echo "Copying ${DATA_FILE} into container ${CONTAINER_NAME}"
docker cp "$DATA_FILE" "${CONTAINER_NAME}:${CONTAINER_PATH}"

if [ $? -ne 0 ]; then
    echo " Failed to copy file into container"
    exit 1
fi

echo "Loading data into ${DATABASE}.${TABLE}"

docker exec -i -e MYSQL_PWD="$PASSWORD" "$CONTAINER_NAME" mysql \
    --user="$USERNAME" \
    --database="$DATABASE" \
    -e "
LOAD DATA INFILE '${CONTAINER_PATH}'
INTO TABLE ${TABLE}
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n';
"

if [ $? -eq 0 ]; then
    echo "Data imported successfully into ${DATABASE}.${TABLE}"
else
    echo "Error during data import. Check connection details, table schema, and CSV format."
    exit 1
fi