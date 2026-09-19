# Investment Portfolio Intelligence

An AWS-based market data pipeline that ingests daily stock prices, calculates
market metrics, and uses Amazon Bedrock to generate concise, data-driven
market insights.

## Current Workflow

1. `src/ingestion/market_data.py` retrieves daily prices for `AAPL`, `MSFT`,
	 `JPM`, and `BLK` from Alpha Vantage and uploads the raw JSON files to S3.
2. `src/transformation/glue_market_prices.py` converts the raw files into an
	 Iceberg `market_prices` table.
3. `src/transformation/glue_market_metrics.py` calculates daily returns,
	 7-day and 30-day returns, a 30-day moving average, and 30-day volatility.
4. `src/ai/market_insights.py` queries the latest metrics through Athena and
	 sends them to Amazon Bedrock for a concise market analysis.

The current AI workflow analyzes market data only. Portfolio holdings,
allocation analysis, and personalized recommendations are not implemented yet.

## AWS Architecture

- **Amazon S3** stores raw market data, Iceberg tables, Glue scripts, and
	Athena query results.
- **AWS Glue** runs the price and metrics transformation jobs.
- **AWS Glue Data Catalog** contains the `investment_portfolio` database.
- **Amazon Athena** queries the latest rows from `market_metrics`.
- **Amazon Bedrock** generates the final market insight using the configured
	foundation model.
- **AWS CDK** defines the S3 bucket, Glue jobs, Glue database, and Athena
	workgroup in `infrastructure/`.

## Repository Structure

```text
investment-portfolio-intelligence/
|
|-- data/
|   |-- raw/                         Sample/local raw data
|   `-- processed/market_prices/     Local Parquet output
|
|-- src/
|   |-- ai/
|   |   `-- market_insights.py       Athena + Bedrock integration
|   |
|   |-- ingestion/
|   |   `-- market_data.py           Alpha Vantage -> S3
|   |
|   |-- transformation/
|   |   |-- market_prices.py         Local PySpark prototype
|   |   |-- glue_market_prices.py    AWS Glue price transformation
|   |   `-- glue_market_metrics.py   AWS Glue metrics transformation
|   |
|   `-- utils/
|
|-- infrastructure/
|   `-- infrastructure/
|       `-- infrastructure_stack.py  AWS CDK stack
|
|-- sql/
|-- tests/
|-- docs/
|-- requirements.txt
|-- .gitignore
`-- README.md
```

## Prerequisites

- Python 3.10 or later
- An AWS account configured for the AWS CLI and boto3
- AWS CDK CLI for infrastructure deployment
- An Alpha Vantage API key
- Access to the selected Amazon Bedrock model in the target region
- Java installed locally when running the PySpark transformation

Install the Python dependencies from the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

The CDK application has its own dependencies:

```powershell
cd infrastructure
python -m pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the repository root. Do not commit it.

```text
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
AWS_REGION=ap-south-1
BEDROCK_MODEL_ID=your_bedrock_model_id
```

`AWS_REGION` must be a region where the deployed resources and Bedrock model
are available. The AWS CLI credentials used by boto3 must also have permission
to access S3, Athena, Glue, and Bedrock.

## Running the Pipeline

### Ingest market data

```powershell
python src/ingestion/market_data.py
```

This uploads files under the `market_prices/` prefix in the configured S3
bucket.

### Run the local transformation

```powershell
python src/transformation/market_prices.py
```

This reads JSON files from `data/raw/` and writes Parquet output to
`data/processed/market_prices/`.

### Generate an AI market insight

After the AWS Glue tables contain data, run:

```powershell
python src/ai/market_insights.py
```

The module queries the latest date in `market_metrics` through Athena and sends
the returned metrics to Bedrock. It prints the generated insight to the
terminal.

## Deploying AWS Infrastructure

From the `infrastructure/` directory:

```powershell
python -m pip install -r requirements.txt
cdk synth
cdk diff
cdk deploy
```

The CDK stack creates the S3 bucket, Glue database, two Glue jobs, and Athena
workgroup. The Glue scripts must be available at the S3 locations referenced
by the stack before the jobs can run.

Useful commands:

```powershell
cdk ls       # List stacks
cdk synth    # Synthesize CloudFormation
cdk diff     # Compare with the deployed stack
cdk deploy   # Deploy the stack
```

## Security Notes

- Keep API keys and AWS credentials out of source control.
- The S3 bucket is configured with blocked public access and server-side
	encryption.
- Review IAM permissions before deploying to a shared AWS account.
- The generated output is an analytical summary, not financial advice.
