#!/bin/bash
# Example: run a query against IBM i via SSH
set -e

HOST="${DB2_HOST:?Set DB2_HOST}"
USER="${DB2_USER:?Set DB2_USER}"
PASSWORD="${DB2_PASSWORD:?Set DB2_PASSWORD}"

source venv/bin/activate

python ssh_query_runner.py \
	--host "$HOST" \
	--user "$USER" \
	--password "$PASSWORD" \
	--query "SELECT TABLE_NAME, TABLE_TYPE FROM QSYS2.SYSTABLES FETCH FIRST 5 ROWS ONLY"
