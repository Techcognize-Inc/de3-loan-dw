#!/usr/bin/env bash
set -e
cd docker
docker-compose up -d
echo "✅ Services started. Postgres: localhost:5432 | Spark UI: http://localhost:8080"