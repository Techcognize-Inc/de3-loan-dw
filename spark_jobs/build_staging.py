from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as fmax, sum as fsum

from common.jdbc import jdbc_url, jdbc_properties


# -----------------------------
# Helpers
# -----------------------------
def quote_ident(name: str) -> str:
    # Postgres identifier quoting
    return f'"{name}"'


def table_columns(spark: SparkSession, table: str) -> list[str]:
    # This is a metadata-only read (Spark typically issues a WHERE 1=0 query)
    return spark.read.jdbc(url=jdbc_url(), table=table, properties=jdbc_properties()).columns


def get_bounds(spark: SparkSession, table: str, partition_col: str) -> tuple[int, int]:
    # IMPORTANT: quote column to preserve uppercase identifiers created by Spark/JDBC
    q = f'(SELECT MIN({quote_ident(partition_col)}) AS lo, MAX({quote_ident(partition_col)}) AS hi FROM {table}) t'
    row = spark.read.jdbc(url=jdbc_url(), table=q, properties=jdbc_properties()).collect()[0]
    lo = row["lo"]
    hi = row["hi"]
    if lo is None or hi is None:
        # empty table or null bounds
        return 0, 1
    return int(lo), int(hi)


def read_table_partitioned(
    spark: SparkSession,
    table: str,
    preferred_partition_cols: list[str],
    columns: list[str],
    num_partitions: int = 16,
):
    """
    Partitioned JDBC read:
    - Picks first partition col that exists in the table
    - Ensures partition col is included in selected columns
    - Uses quoted bounds query (MIN/MAX) to avoid case issues
    """
    cols_in_table = table_columns(spark, table)

    # pick a partition col that exists
    partition_col = None
    for c in preferred_partition_cols:
        if c in cols_in_table:
            partition_col = c
            break
    if partition_col is None:
        # last resort: no partitioning
        keep = [c for c in columns if c in cols_in_table]
        return (
            spark.read
            .jdbc(url=jdbc_url(), table=table, properties=jdbc_properties())
            .select(*keep)
        )

    # Ensure partition column included
    select_cols = list(dict.fromkeys(columns + [partition_col]))  # preserve order, de-dupe
    select_cols = [c for c in select_cols if c in cols_in_table]
    quoted_cols = ", ".join([quote_ident(c) for c in select_cols])

    # Use subquery so we can control selected columns (and keep partition col present)
    subquery = f'(SELECT {quoted_cols} FROM {table}) t'

    lo, hi = get_bounds(spark, table, partition_col)

    # Important: Spark "partitionColumn" refers to column name as returned by JDBC relation.
    # Since our subquery selects "SK_ID_..." with quotes, Spark sees them as SK_ID_... (same name).
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", subquery)
        .option("user", jdbc_properties().get("user"))
        .option("password", jdbc_properties().get("password"))
        .option("driver", jdbc_properties().get("driver"))
        .option("partitionColumn", partition_col)
        .option("lowerBound", lo)
        .option("upperBound", hi)
        .option("numPartitions", num_partitions)
        .load()
    )


def write_table_jdbc(df, table: str, mode: str = "overwrite"):
    (
        df.write
        .mode(mode)
        .option("batchsize", 20000)
        .jdbc(url=jdbc_url(), table=table, properties=jdbc_properties())
    )


# -----------------------------
# Main
# -----------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("de3-build-staging")
        # helpful defaults (can still be overridden by spark-submit --conf)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    # Base application table (one row per loan application)
    app = read_table_partitioned(
        spark,
        table="raw.application_train",
        preferred_partition_cols=["SK_ID_CURR"],
        columns=[
            "SK_ID_CURR",
            "TARGET",
            "NAME_CONTRACT_TYPE",
            "CODE_GENDER",
            "FLAG_OWN_CAR",
            "FLAG_OWN_REALTY",
            "CNT_CHILDREN",
            "AMT_INCOME_TOTAL",
            "AMT_CREDIT",
            "AMT_ANNUITY",
            "AMT_GOODS_PRICE",
        ],
        num_partitions=16,
    )

    # Bureau (partition by SK_ID_BUREAU if present, fallback to SK_ID_CURR)
    bureau = read_table_partitioned(
        spark,
        table="raw.bureau",
        preferred_partition_cols=["SK_ID_BUREAU", "SK_ID_CURR"],
        columns=[
            "SK_ID_BUREAU",        # ✅ MUST include if partitioning by it
            "SK_ID_CURR",
            "AMT_CREDIT_SUM",
            "CREDIT_DAY_OVERDUE",
        ],
        num_partitions=16,
    )

    bureau_agg = (
        bureau.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("bureau_records_cnt"),
            avg(col("AMT_CREDIT_SUM")).alias("bureau_avg_credit_sum"),
            fmax(col("AMT_CREDIT_SUM")).alias("bureau_max_credit_sum"),
            avg(col("CREDIT_DAY_OVERDUE")).alias("bureau_avg_days_overdue"),
            fmax(col("CREDIT_DAY_OVERDUE")).alias("bureau_max_days_overdue"),
        )
    )

    # Previous applications (prefer SK_ID_PREV if exists, else SK_ID_CURR)
    prev = read_table_partitioned(
        spark,
        table="raw.previous_application",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_APPLICATION",
            "AMT_CREDIT",
        ],
        num_partitions=16,
    )

    prev_agg = (
        prev.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("prev_app_cnt"),
            avg(col("AMT_APPLICATION")).alias("prev_avg_amt_application"),
            avg(col("AMT_CREDIT")).alias("prev_avg_amt_credit"),
        )
    )

    # POS cash balance (prefer SK_ID_PREV, else SK_ID_CURR)
    pos = read_table_partitioned(
        spark,
        table="raw.pos_cash_balance",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "SK_DPD",
        ],
        num_partitions=16,
    )

    pos_agg = (
        pos.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("pos_records_cnt"),
            avg(col("SK_DPD")).alias("pos_avg_dpd"),
            fmax(col("SK_DPD")).alias("pos_max_dpd"),
        )
    )

    # Installments payments (prefer SK_ID_PREV, else SK_ID_CURR)
    inst = read_table_partitioned(
        spark,
        table="raw.installments_payments",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_PAYMENT",
        ],
        num_partitions=16,
    )

    inst_agg = (
        inst.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("inst_records_cnt"),
            fsum(col("AMT_PAYMENT")).alias("inst_total_amt_paid"),
            avg(col("AMT_PAYMENT")).alias("inst_avg_amt_paid"),
        )
    )

    # Credit card balance (prefer SK_ID_PREV, else SK_ID_CURR)
    cc = read_table_partitioned(
        spark,
        table="raw.credit_card_balance",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_BALANCE",
        ],
        num_partitions=16,
    )

    cc_agg = (
        cc.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("cc_records_cnt"),
            avg(col("AMT_BALANCE")).alias("cc_avg_balance"),
            fmax(col("AMT_BALANCE")).alias("cc_max_balance"),
        )
    )

    # Join all features to application table (left joins keep all apps)
    stg = (
        app
        .join(bureau_agg, on="SK_ID_CURR", how="left")
        .join(prev_agg, on="SK_ID_CURR", how="left")
        .join(pos_agg, on="SK_ID_CURR", how="left")
        .join(inst_agg, on="SK_ID_CURR", how="left")
        .join(cc_agg, on="SK_ID_CURR", how="left")
    )

    # Keep base cols + all new agg cols
    base_keep = [
        "SK_ID_CURR",
        "TARGET",
        "NAME_CONTRACT_TYPE",
        "CODE_GENDER",
        "FLAG_OWN_CAR",
        "FLAG_OWN_REALTY",
        "CNT_CHILDREN",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "AMT_ANNUITY",
        "AMT_GOODS_PRICE",
    ]
    existing_keep = [c for c in base_keep if c in stg.columns]
    agg_cols = [c for c in stg.columns if c not in app.columns]  # new features only

    final_df = stg.select(*(existing_keep + agg_cols))

    # Reduce skew before write (helps stability)
    final_df = final_df.repartition(16, col("SK_ID_CURR"))

    # Write to staging
    write_table_jdbc(final_df, "staging.stg_loan_application_enriched", mode="overwrite")

    spark.stop()
    print("✅ Staging table created: staging.stg_loan_application_enriched")


if __name__ == "__main__":
    main()