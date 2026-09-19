import os
import re
import time

import boto3
from dotenv import load_dotenv


load_dotenv()

REGION = os.getenv("AWS_REGION", "ap-south-1")
MODEL_ID = os.getenv("BEDROCK_MODEL_ID")
DATABASE = "investment_portfolio"
OUTPUT_LOCATION = "s3://investment-portfolio-raw-data/athena-results/"

SCHEMA = """
Table: market_metrics

Columns:
- symbol STRING
- price_date DATE
- close DOUBLE
- volume BIGINT
- daily_return DOUBLE
- return_7d DOUBLE
- return_30d DOUBLE
- moving_avg_30d DOUBLE
- volatility_30d DOUBLE
"""

bedrock = boto3.client(
    "bedrock-runtime",
    region_name=REGION,
)

athena = boto3.client(
    "athena",
    region_name=REGION,
)

QUERY_RULES = """
Query behavior rules:

1. For current/latest/latest available market conditions,
   filter using:

   WHERE price_date = (
       SELECT MAX(price_date)
       FROM market_metrics
   )

2. For questions like:
   - "which stocks are trading..."
   - "show me stocks..."
   - "what is the current..."
   use the latest available price_date unless historical data is
   explicitly requested.

3. For comparisons across stocks, return one row per stock for the
   latest available date unless a historical period is requested.

4. Do not return unnecessary historical rows.

5. Use Amazon Athena SQL syntax only.

6. Do NOT use QUALIFY.

7. For latest-date filtering, always use MAX(price_date) in a
   subquery.

8. Generate simple SQL. Avoid complex SQL constructs when a basic
   WHERE, GROUP BY, ORDER BY, or subquery can solve the question.
"""

conversation_history = []

def execute_query(sql: str):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )

    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        state = result["QueryExecution"]["Status"]["State"]

        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    if state != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get(
            "StateChangeReason",
            "Unknown error"
        )
        return {
            "success": False,
            "error": reason,
        }

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


    return {
    "success": True,
    "data": data,
    }


def generate_sql(question: str) -> str:

    previous_context = ""

    if conversation_history:
        previous_context = f"""
Previous conversation:
{conversation_history[-3:]}
"""

    prompt = f"""
You are a SQL generation assistant for an investment market analytics system.

Database: investment_portfolio
Table: market_metrics

{SCHEMA}

{QUERY_RULES}

{previous_context}

Current user question:
{question}

Use the previous conversation to resolve references such as:
- it
- its
- that stock
- that company
- the same stock

If the current question depends on a previous result, use that
information when generating the SQL.

Return ONLY the SQL query.
Do not return explanations, headings, comments, markdown, or code fences.
"""

    response = bedrock.converse(
        modelId=MODEL_ID,
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ],
        inferenceConfig={
            "maxTokens": 300,
            "temperature": 0,
        },
    )

    return response["output"]["message"]["content"][0]["text"].strip()

def extract_sql(text: str) -> str:
    """
    Extract SQL from an LLM response that may contain
    markdown, explanations, or surrounding text.
    """

    # Prefer SQL code block if present
    match = re.search(
        r"```sql\s*(.*?)```",
        text,
        flags=re.IGNORECASE | re.DOTALL
    )

    if match:
        return match.group(1).strip()

    # Fallback: find first SELECT statement
    match = re.search(
        r"\bSELECT\b.*",
        text,
        flags=re.IGNORECASE | re.DOTALL
    )

    if match:
        return match.group(0).strip()

    raise ValueError("No SQL SELECT statement found in LLM response.")

def validate_sql(sql: str) -> str:
    sql = sql.strip().rstrip(";")

    # Remove markdown fences if model accidentally adds them
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```$", "", sql).strip()

    # Only SELECT queries
    if not sql.lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    # Block multiple statements
    if ";" in sql:
        raise ValueError("Multiple SQL statements are not allowed.")

    # Block dangerous SQL operations
    forbidden = [
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "truncate",
        "merge",
    ]

    for keyword in forbidden:
        if re.search(rf"\b{keyword}\b", sql, re.IGNORECASE):
            raise ValueError(f"Forbidden SQL operation: {keyword}")

    # Allow only our intended table
    if not re.search(r"\bmarket_metrics\b", sql, re.IGNORECASE):
        raise ValueError("Only market_metrics table is allowed.")

    return sql

def generate_answer(question: str, sql: str, results: list) -> str:

    prompt = f"""
You are an AI data analyst.

Answer the user's question using ONLY the Athena query result provided below.

User question:
{question}

SQL executed:
{sql}

Athena result:
{results}

Rules:
- Use only the provided result.
- Do not invent numbers or facts.
- If the result is empty, clearly say that no matching data was found.
- Keep the answer concise and easy to understand.
- Do not provide financial advice.
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
            "maxTokens": 250,
            "temperature": 0
        }
    )

    return response["output"]["message"]["content"][0]["text"].strip()

if __name__ == "__main__":

    while True:

        question = input("\nAsk a question (type 'exit' to quit): ")

        if question.lower() == "exit":
            break

        generated_response = generate_sql(question)

        print("\n===== RAW LLM RESPONSE =====\n")
        print(generated_response)

        generated_sql = extract_sql(generated_response)

        print("\n===== EXTRACTED SQL =====\n")
        print(generated_sql)

        validated_sql = validate_sql(generated_sql)

        print("\n===== EXECUTING ATHENA QUERY =====")

        results = execute_query(validated_sql)

        print("\n===== QUERY RESULTS =====\n")

        for row in results:
            print(row)

        answer = generate_answer(
            question,
            validated_sql,
            results,
        )

        print("\n===== AI ANSWER =====\n")
        print(answer)

        conversation_history.append({
            "question": question,
            "sql": validated_sql,
            "results": results,
            "answer": answer,
        })