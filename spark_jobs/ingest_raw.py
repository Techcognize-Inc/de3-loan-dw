from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from common.jdbc import jdbc_url, jdbc_properties

TABLE_MAP = {
    "application_train.csv": "application_train",
    "bureau.csv": "bureau",
    "bureau_balance.csv": "bureau_balance",
    "previous_application.csv": "previous_application",
    "POS_CASH_balance.csv": "pos_cash_balance",
    "installments_payments.csv": "installments_payments",
    "credit_card_balance.csv": "credit_card_balance",
}

RAW_PATH = "/opt/spark/work-dir/data/raw"

def main():
    spark = SparkSession.builder.appName("de3-ingest-raw").getOrCreate()

    for file_name, table_name in TABLE_MAP.items():
        path = f"{RAW_PATH}/{file_name}"
        print(f"\n=== Reading: {path}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")   # OK for Day 2; we’ll harden later
            .csv(path)
        )

        df = df.select([col(c).alias(c.strip()) for c in df.columns])

        full_table = f"raw.{table_name}"
        print(f"Writing to {full_table} | rows={df.count()} | cols={len(df.columns)}")

        (
            df.write
            .mode("overwrite")
            .jdbc(url=jdbc_url(), table=full_table, properties=jdbc_properties())
        )

    spark.stop()
    print("\n✅ Raw ingestion complete.")

if __name__ == "__main__":
    main()