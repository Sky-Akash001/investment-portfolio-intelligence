import json
import os
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = os.sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = os.sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)
from pyspark.sql.functions import col, to_date


RAW_DIR = "data/raw"
OUTPUT_DIR = "data/processed/market_prices"


def main():
    spark = (
        SparkSession.builder
        .appName("MarketPriceTransformation")
        .master("local[*]")
        .getOrCreate()
    )

    rows = []

    for file_path in Path(RAW_DIR).glob("*.json"):
        with open(file_path, "r") as file:
            data = json.load(file)

        symbol = data["Meta Data"]["2. Symbol"]
        time_series = data["Time Series (Daily)"]

        for price_date, values in time_series.items():
            rows.append(
                (
                    symbol,
                    price_date,
                    float(values["1. open"]),
                    float(values["2. high"]),
                    float(values["3. low"]),
                    float(values["4. close"]),
                    int(values["5. volume"]),
                )
            )

    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price_date", StringType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", LongType(), False),
    ])

    df = spark.createDataFrame(rows, schema)

    df = df.withColumn(
        "price_date",
        to_date(col("price_date"), "yyyy-MM-dd")
    )

    df.show(20, truncate=False)
    df.printSchema()

    df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"Processed {df.count()} records.")

    spark.stop()


if __name__ == "__main__":
    main()