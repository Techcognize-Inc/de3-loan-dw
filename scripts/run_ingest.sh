#!/usr/bin/env bash
set -euo pipefail

echo "Running raw ingestion: raw.* tables"

docker exec -it de3-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --jars /opt/spark/work-dir/jars/postgresql.jar \
  /opt/spark/work-dir/spark_jobs/ingest_raw.py

echo "✅ Raw ingestion finished."