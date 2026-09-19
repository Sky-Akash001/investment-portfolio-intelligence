import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)


SOURCE_TABLE = "glue_catalog.investment_portfolio.market_prices"
TARGET_TABLE = "glue_catalog.investment_portfolio.market_metrics"

df = spark.table(SOURCE_TABLE)

window = Window.partitionBy("symbol").orderBy("price_date")

df = (
    df
    .withColumn("previous_close", F.lag("close", 1).over(window))
    .withColumn("close_7d_ago", F.lag("close", 7).over(window))
    .withColumn("close_30d_ago", F.lag("close", 30).over(window))
    .withColumn(
        "daily_return",
        (F.col("close") - F.col("previous_close"))
        / F.col("previous_close")
    )
    .withColumn(
        "return_7d",
        (F.col("close") - F.col("close_7d_ago"))
        / F.col("close_7d_ago")
    )
    .withColumn(
        "return_30d",
        (F.col("close") - F.col("close_30d_ago"))
        / F.col("close_30d_ago")
    )
    .withColumn(
        "moving_avg_30d",
        F.avg("close").over(
            window.rowsBetween(-29, 0)
        )
    )
    .withColumn(
        "volatility_30d",
        F.stddev("daily_return").over(
            window.rowsBetween(-29, 0)
        )
    )
    .select(
        "symbol",
        "price_date",
        "close",
        "volume",
        "daily_return",
        "return_7d",
        "return_30d",
        "moving_avg_30d",
        "volatility_30d",
    )
)


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
    (
        symbol STRING,
        price_date DATE,
        close DOUBLE,
        volume BIGINT,
        daily_return DOUBLE,
        return_7d DOUBLE,
        return_30d DOUBLE,
        moving_avg_30d DOUBLE,
        volatility_30d DOUBLE
    )
    USING iceberg
    PARTITIONED BY (days(price_date))
    LOCATION 's3://investment-portfolio-raw-data/iceberg/market_metrics/'
""")


df.writeTo(TARGET_TABLE).overwritePartitions()

print(f"Processed {df.count()} market metric records.")

job.commit()