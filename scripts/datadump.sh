#!/bin/bash
DB_HOST="127.0.0.1"
DB_PORT="3308"
DB_NAME="softcart_sales"
DB_USER="root"
OUTPUT_FILE="sales_data.sql"

echo "Enter MySQL password:"
read -s DB_PASSWORD
echo ""

mysqldump \
  -h "$DB_HOST" \
  -P "$DB_PORT" \
  -u "$DB_USER" \
  -p"$DB_PASSWORD" \
  "$DB_NAME" sales_data > "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
  echo "Data export successful."
else
  echo "Data export failed."
fi