"""Gold: Monthly compensation trend."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="gold_monthly_compensation_trend",
    comment="Monthly compensation trend.",
)
def gold_monthly_compensation_trend():
    fact = spark.read.table("gold_fact_daily_compensation")
    return fact.withColumn(
        "month", F.date_format("closure_date", "yyyy-MM")
    ).groupBy("month").agg(
        F.sum("daily_compensation_ils").alias("total_compensation_ils"),
        F.countDistinct("id_esek").alias("num_businesses_affected"),
        F.countDistinct("closure_date").alias("num_closure_days"),
    ).orderBy("month")
