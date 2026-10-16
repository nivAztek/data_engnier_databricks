"""Gold: Annual compensation totals per street."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="gold_compensation_per_street",
    comment="Annual compensation totals aggregated per street.",
    cluster_by=["shem_rechov"],
)
def gold_compensation_per_street():
    biz = spark.read.table("gold_compensation_per_business")
    return biz.groupBy("shem_rechov").agg(
        F.sum("total_compensation_ils").alias("total_street_compensation_ils"),
        F.count("id_esek").alias("num_businesses_affected"),
        F.avg("total_compensation_ils").alias("avg_compensation_per_business_ils"),
        F.max("total_closure_days").alias("max_closure_days"),
        F.sum("total_closure_days").alias("total_business_closure_days"),
    )
