import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = sys.argv

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)

RAW_PATH = "s3://investment-portfolio-raw-data/market_prices/"
PROCESSED_PATH = "s3://investment-portfolio-raw-data/processed/market_prices/"

df = spark.read.json(RAW_PATH)

df = df.select(
    F.regexp_extract(
        F.input_file_name(),
        r"/([A-Z]+)_\d{8}T\d{6}Z\.json$",
        1
    ).alias("symbol"),
    F.from_json(
        F.to_json(F.col("`Time Series (Daily)`")),
        "map<string,struct<`1. open`:string,`2. high`:string,`3. low`:string,`4. close`:string,`5. volume`:string>>"
    ).alias("daily_prices")
)

df = df.select(
    "symbol",
    F.explode("daily_prices").alias("price_date", "price_data")
)

df = df.select(
    "symbol",
    F.to_date("price_date").alias("price_date"),
    F.col("price_data.`1. open`").cast("double").alias("open"),
    F.col("price_data.`2. high`").cast("double").alias("high"),
    F.col("price_data.`3. low`").cast("double").alias("low"),
    F.col("price_data.`4. close`").cast("double").alias("close"),
    F.col("price_data.`5. volume`").cast("long").alias("volume")
)
df.createOrReplaceTempView("market_prices_temp")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.investment_portfolio.market_prices
    (
        symbol STRING,
        price_date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT
    )
    USING iceberg
    PARTITIONED BY (days(price_date))
    LOCATION 's3://investment-portfolio-raw-data/iceberg/market_prices/'
""")

df.writeTo(
    "glue_catalog.investment_portfolio.market_prices"
).append()

job.commit()
