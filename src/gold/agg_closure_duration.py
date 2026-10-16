"""Gold: Closure duration analysis per street."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

YEAR_START = spark.conf.get("pipeline.year_start", "2023-01-01")
YEAR_END = spark.conf.get("pipeline.year_end", "2023-12-31")


@dp.materialized_view(
    name="gold_closure_duration_by_street",
    comment="Closure duration and frequency per street.",
)
def gold_closure_duration_by_street():
    closures = spark.read.table("silver_street_closures")
    closures_yr = closures.filter(
        (F.col("closure_start") <= F.lit(YEAR_END))
        & (F.col("closure_end") >= F.lit(YEAR_START))
    ).withColumn(
        "duration_days",
        F.datediff(
            F.least(F.to_date("closure_end"), F.to_date(F.lit(YEAR_END))),
            F.greatest(F.to_date("closure_start"), F.to_date(F.lit(YEAR_START))),
        ) + 1,
    )
    return closures_yr.groupBy("affected_street").agg(
        F.count("closure_id").alias("num_closure_events"),
        F.avg("duration_days").alias("avg_closure_duration_days"),
        F.max("duration_days").alias("max_closure_duration_days"),
        F.sum("duration_days").alias("total_closure_days"),
        F.collect_set("t_sug").alias("closure_types"),
    ).orderBy(F.desc("total_closure_days"))
