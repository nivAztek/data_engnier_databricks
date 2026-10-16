"""Gold: Cost-per-area metric."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="gold_cost_per_area",
    comment="Compensation normalized by business area (shetach).",
)
def gold_cost_per_area():
    biz = spark.read.table("gold_compensation_per_business")
    return biz.withColumn(
        "cost_per_area",
        F.when(F.col("shetach") > 0,
               F.col("total_compensation_ils") / F.col("shetach")
        ).otherwise(0.0),
    ).withColumn(
        "daily_rate_applied",
        F.least(F.col("shetach") * 100, F.lit(10000)).cast("double"),
    ).select(
        "id_esek", "shimush", "shem_rechov", "shetach",
        "total_compensation_ils", "total_closure_days",
        "cost_per_area", "daily_rate_applied",
    )
