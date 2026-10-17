"""Glue job: merge new DMS CDC files for one table and one tenant batch into silver Iceberg.

Arguments:
    --table          table name from conf/tables.json
    --batch_index    which tenant batch this run owns (0-based)
    --batch_count    total number of batches the tenants are split into
    --lake_bucket    S3 bucket holding bronze/ and _state/
    --bronze_prefix  e.g. bronze/
    --config_uri     s3://.../tables.json

Step Functions starts one run per (table, batch), so parallelism stays within the
Glue job's MaxConcurrentRuns no matter how many source databases exist.
"""

import json
import sys
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from lakehouse import files
from lakehouse import sql as q
from lakehouse.spec import parse_specs

CATALOG = "glue_catalog"

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "table", "batch_index", "batch_count", "lake_bucket", "bronze_prefix", "config_uri"],
)
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
log = glue_context.get_logger()
s3 = boto3.client("s3")

bucket = args["lake_bucket"]
bronze_prefix = args["bronze_prefix"].rstrip("/") + "/"


def read_config(uri: str) -> dict:
    parsed = urlparse(uri)
    body = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"].read()
    return parse_specs(json.loads(body))


def list_keys(prefix: str, delimiter: str = None):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    if delimiter:
        kwargs["Delimiter"] = delimiter
    for page in paginator.paginate(**kwargs):
        if delimiter:
            yield from (p["Prefix"] for p in page.get("CommonPrefixes", []))
        else:
            yield from (o["Key"] for o in page.get("Contents", []))


def state_key(table: str, tenant: str) -> str:
    return f"_state/{table}/tenant={tenant}.json"


def read_watermark(table: str, tenant: str):
    try:
        body = s3.get_object(Bucket=bucket, Key=state_key(table, tenant))["Body"].read()
    except ClientError as err:
        if err.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(body)["watermark"]


def write_watermark(table: str, tenant: str, watermark: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=state_key(table, tenant),
        Body=json.dumps({"watermark": watermark}).encode(),
        ContentType="application/json",
    )


spec = read_config(args["config_uri"])[args["table"]]
target = f"{CATALOG}.silver.{spec.name}"
all_tenants = [t for t in (files.tenant_from_prefix(p) for p in list_keys(bronze_prefix, "/")) if t]
tenants = files.select_batch(all_tenants, int(args["batch_index"]), int(args["batch_count"]))
log.info(f"{spec.name}: batch {args['batch_index']}/{args['batch_count']} -> {len(tenants)} tenants")

spark.sql(q.create_table_sql(spec, target, q.SPARK))

parts, watermarks = [], {}
for i, tenant in enumerate(tenants):
    prefix = files.table_prefix(bronze_prefix, tenant, spec.source_schema, spec.name)
    selected, new_wm = files.select_new_files(list_keys(prefix), read_watermark(spec.name, tenant))
    if not selected:
        continue
    view = f"raw_{i}"
    (
        spark.read.option("mergeSchema", "true")
        .parquet(*[f"s3://{bucket}/{k}" for k in selected])
        .withColumn("_src_file", F.input_file_name())
        # Row position tie-breaker for changes sharing one commit timestamp.
        .withColumn("_row", F.monotonically_increasing_id())
        .createOrReplaceTempView(view)
    )
    columns = spark.table(view).columns
    parts.append(q.projection_sql(spec, view, columns, tenant))
    watermarks[tenant] = new_wm
    log.info(f"{spec.name}/{tenant}: {len(selected)} new file(s)")

if parts:
    spark.sql(" UNION ALL ".join(parts)).createOrReplaceTempView("staged_raw")
    spark.sql(q.dedup_sql(spec, "staged_raw")).createOrReplaceTempView("staged")
    # One MERGE (one Iceberg commit) per batch, scoped to this batch's tenants.
    spark.sql(q.merge_sql(spec, target, "staged", tenants=sorted(watermarks)))
    for tenant, wm in watermarks.items():
        write_watermark(spec.name, tenant, wm)
else:
    log.info(f"{spec.name}: no new files for this batch")

job.commit()
