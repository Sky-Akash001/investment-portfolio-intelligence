from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    aws_s3_deployment as s3deploy
)
from constructs import Construct


class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            bucket_name="investment-portfolio-raw-data",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
        )

        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        raw_bucket.grant_read_write(glue_role)

        glue_job = glue.CfnJob(
            self,
            "MarketPricesGlueJob",
            name="investment-market-prices-job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location="s3://investment-portfolio-raw-data/scripts/glue_market_prices.py",
            ),
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            default_arguments={
                "--datalake-formats": "iceberg",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    "--conf spark.sql.catalog.glue_catalog.warehouse=s3://investment-portfolio-raw-data/iceberg/"
                ),
            },
        )

        metrics_job = glue.CfnJob(
            self,
            "MarketMetricsGlueJob",
            name="investment-market-metrics-job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    "s3://investment-portfolio-raw-data/"
                    "scripts/glue_market_metrics.py"
                ),
            ),
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            default_arguments={
                "--datalake-formats": "iceberg",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    "--conf spark.sql.catalog.glue_catalog.warehouse=s3://investment-portfolio-raw-data/iceberg/"
                ),
            },
        )

        glue_database = glue.CfnDatabase(
            self,
            "InvestmentDataDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="investment_portfolio"
            ),
        )

        athena_workgroup = athena.CfnWorkGroup(
            self,
            "InvestmentAthenaWorkGroup",
            name="investment-portfolio-workgroup",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location="s3://investment-portfolio-raw-data/athena-results/"
                )
            )
        )

