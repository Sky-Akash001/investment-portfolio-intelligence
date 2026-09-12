import os
import json
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BUCKET_NAME = "investment-portfolio-raw-data"

SYMBOLS = ["AAPL", "MSFT", "JPM", "BLK"]


def fetch_market_data(symbol: str) -> dict:
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def upload_to_s3(data: dict, symbol: str) -> None:
    s3 = boto3.client("s3")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    key = (
        f"market_prices/"
        f"ingestion_date={datetime.now(timezone.utc).date()}/"
        f"{symbol}_{timestamp}.json"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )

    print(f"Uploaded: s3://{BUCKET_NAME}/{key}")


def main():
    if not API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY not found in .env")

    for symbol in SYMBOLS:
        print(f"Fetching {symbol}...")

        data = fetch_market_data(symbol)

        if "Error Message" in data:
            print(f"API error for {symbol}: {data['Error Message']}")
            continue

        if "Note" in data:
            print(f"API limit/message: {data['Note']}")
            continue

        upload_to_s3(data, symbol)


if __name__ == "__main__":
    main()
