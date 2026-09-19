import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-south-1")
MODEL_ID = os.getenv("BEDROCK_MODEL_ID")

athena = boto3.client("athena", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION)

DATABASE = "investment_portfolio"
OUTPUT_LOCATION = "s3://investment-portfolio-raw-data/athena-results/"


def get_market_data():
    query = """
    SELECT
        symbol,
        price_date,
        close,
        daily_return,
        return_7d,
        return_30d,
        moving_avg_30d,
        volatility_30d
    FROM market_metrics
    WHERE price_date = (
        SELECT MAX(price_date)
        FROM market_metrics
    )
    ORDER BY symbol
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )

    query_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    if status != "SUCCEEDED":
        raise RuntimeError(f"Athena query failed: {status}")

    result = athena.get_query_results(
        QueryExecutionId=query_id
    )

    rows = result["ResultSet"]["Rows"]

    headers = [
        cell.get("VarCharValue", "")
        for cell in rows[0]["Data"]
    ]

    data = []

    for row in rows[1:]:
        values = [
            cell.get("VarCharValue", "")
            for cell in row["Data"]
        ]

        data.append(dict(zip(headers, values)))

    return data


def generate_insight(market_data):

    prompt = f"""
You are a financial data analyst.

Analyze these actual market metrics:

{market_data}

Provide:
1. Overall market observation
2. Notable stock-level trends
3. Volatility observations
4. Important data-driven risks

Do not provide financial advice.
Do not invent information that is not present in the data.
Keep the response concise.
"""

    response = bedrock.converse(
        modelId=MODEL_ID,
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}]
            }
        ],
        inferenceConfig={
            "maxTokens": 400,
            "temperature": 0.2
        }
    )

    return response["output"]["message"]["content"][0]["text"]


if __name__ == "__main__":

    market_data = get_market_data()

    print("\n===== MARKET DATA =====")
    print(market_data)

    insight = generate_insight(market_data)

    print("\n===== AI MARKET INSIGHT =====\n")
    print(insight)