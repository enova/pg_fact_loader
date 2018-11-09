#!/usr/bin/env bash

set -eu

./audit.sh test customers customer_id >> sql/03_audit.sql
./audit.sh test email_promos email_promo_id >> sql/03_audit.sql
./audit.sh test emails email_id >> sql/03_audit.sql
./audit.sh test order_product_promos order_product_promo_id >> sql/03_audit.sql
./audit.sh test order_products order_product_id >> sql/03_audit.sql
./audit.sh test orders order_id >> sql/03_audit.sql
./audit.sh test products product_id >> sql/03_audit.sql
./audit.sh test promos promo_id >> sql/03_audit.sql
./audit.sh test reorders reorder_id >> sql/03_audit.sql
