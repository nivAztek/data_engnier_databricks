"""Gold: Annual compensation totals per business."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="gold_compensation_per_business",
    comment="Annual compensation totals per business.",
    cluster_by=["shem_rechov"],
)
def gold_compensation_per_business():
    fact = spark.read.table("gold_fact_daily_compensation")
    return fact.groupBy("id_esek", "shimush", "shem_rechov", "shetach").agg(
        F.sum("daily_compensation_ils").alias("total_compensation_ils"),
        F.count("closure_date").alias("total_closure_days"),
        F.min("closure_date").alias("first_closure_date"),
        F.max("closure_date").alias("last_closure_date"),
    )
