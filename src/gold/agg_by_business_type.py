"""Gold: Compensation breakdown by business type."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="gold_compensation_by_business_type",
    comment="Compensation breakdown by business type (shimush).",
)
def gold_compensation_by_business_type():
    biz = spark.read.table("gold_compensation_per_business")
    return biz.groupBy("shimush").agg(
        F.count("id_esek").alias("num_businesses"),
        F.sum("total_compensation_ils").alias("total_compensation_ils"),
        F.avg("total_compensation_ils").alias("avg_compensation_ils"),
        F.sum("total_closure_days").alias("total_closure_days"),
        F.avg("shetach").alias("avg_area_shetach"),
    ).orderBy(F.desc("total_compensation_ils"))
