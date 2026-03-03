#!/usr/bin/env bash
set -e
echo "Initializing schemas in Postgres..."
docker exec -i de3-postgres psql -U postgres -d bankingdb < sql/init.sql
echo "✅ Schemas created: raw, staging, warehouse"