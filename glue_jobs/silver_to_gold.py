"""Glue job: rebuild the gold Iceberg tables from the SQL models in s3://<artifacts>/gold/.

Models run in file-name order (``01_``, ``02_`` ...), so later models may read earlier ones.
The same .sql files run on DuckDB in the local pipeline and tests.
"""

import sys
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from lakehouse import sql as q

args = getResolvedOptions(sys.argv, ["JOB_NAME", "gold_models_uri"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
log = glue_context.get_logger()
s3 = boto3.client("s3")

uri = urlparse(args["gold_models_uri"])
bucket, prefix = uri.netloc, uri.path.lstrip("/").rstrip("/") + "/"
keys = sorted(
    o["Key"]
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    for o in page.get("Contents", [])
    if o["Key"].endswith(".sql")
)

for key in keys:
    name = q.gold_model_name(key.rsplit("/", 1)[-1][: -len(".sql")])
    select_sql = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    log.info(f"building gold.{name} from {key}")
    spark.sql(q.ctas_sql(f"glue_catalog.gold.{name}", select_sql, q.SPARK))

job.commit()
