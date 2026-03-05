#!/bin/bash

# ──────────────────────────────────────────────
# MySQL Data Import Script
# Reads connection details from config_file.ini
# ──────────────────────────────────────────────

CONFIG_FILE="$(dirname "$0")/../resources/config_file.ini"
DATA_FILE="$(dirname "$0")/../data/sales.csv"


# ──────────────────────────────────────────────
# Parse config.ini for a specific [section] + key
# ──────────────────────────────────────────────
parse_config() {
    local section=$1
    local key=$2
    awk -F':' \
        "/^\[${section}\]/{f=1; next} \
         f && /^\[/{f=0} \
         f && /^${key}[[:space:]]*:/{print \$2; exit}" \
        "$CONFIG_FILE" | tr -d ' '
}

HOST=$(parse_config "mysql" "host")
PORT=$(parse_config "mysql" "port")
USERNAME=$(parse_config "mysql" "user")
PASSWORD=$(parse_config "mysql" "root_password")   # uses root_password from config
ROOT_USER="root"
DATABASE=$(parse_config "mysql" "database")
TABLE="sales_data"

# ──────────────────────────────────────────────
# Validate files exist before doing anything
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
# Debug: print parsed values
# ──────────────────────────────────────────────
echo "──────────────────────────────────────────────"
echo " Config parsed:"
echo "   HOST     : $HOST"
echo "   PORT     : $PORT"
echo "   USER     : $ROOT_USER"
echo "   DATABASE : $DATABASE"
echo "──────────────────────────────────────────────"

# ──────────────────────────────────────────────
# Grant FILE privilege to root (required for LOAD DATA INFILE)
# ──────────────────────────────────────────────
CONTAINER_NAME="mysql_db"
CONTAINER_PATH="/var/lib/mysql-files/oltpdata.csv"

echo "Granting FILE privilege to ${ROOT_USER}..."
docker exec -i "$CONTAINER_NAME" mysql \
    -u "$ROOT_USER" \
    -p"$PASSWORD" \
    -e "GRANT FILE ON *.* TO 'softcart_user'@'%'; FLUSH PRIVILEGES;" 2>/dev/null

# ──────────────────────────────────────────────
# Copy CSV into container and load
# ──────────────────────────────────────────────
echo "Copying ${DATA_FILE} into container ${CONTAINER_NAME}..."
docker cp "$DATA_FILE" "${CONTAINER_NAME}:${CONTAINER_PATH}"

if [ $? -ne 0 ]; then
    echo "Failed to copy file into container"
    exit 1
fi

echo "Loading data into ${DATABASE}.${TABLE}..."

docker exec -i "$CONTAINER_NAME" mysql \
    -u "$ROOT_USER" \
    -p"$PASSWORD" \
    --database="$DATABASE" \
    -e "
LOAD DATA INFILE '${CONTAINER_PATH}'
INTO TABLE ${TABLE}
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
"

if [ $? -eq 0 ]; then
    echo "Data imported successfully into ${DATABASE}.${TABLE}"
else
    echo "Error during data import. Check connection details, table schema, and CSV format."
    exit 1
fi