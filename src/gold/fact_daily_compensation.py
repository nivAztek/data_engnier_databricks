"""Gold: Central fact table - one row per (business, closure_date).
This is the SINGLE source of truth for all compensation aggregations.
All downstream gold tables read from this fact.

Business Rules:
1. Business eligible when its street appears in closure data.
2. One compensation event per business per day.
3. daily_compensation = MIN(shetach * 100, 10000) ILS.
4. Both streets per closure are affected (handled in silver).
5. 2023 only (configurable via pipeline.year_start/end).
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

YEAR_START = spark.conf.get("pipeline.year_start", "2023-01-01")
YEAR_END = spark.conf.get("pipeline.year_end", "2023-12-31")


@dp.materialized_view(
    name="gold_fact_daily_compensation",
    comment="Grain: one row per (id_esek, closure_date). Central compensation fact.",
    cluster_by=["closure_date", "shem_rechov"],
)
@dp.expect("positive_compensation", "daily_compensation_ils > 0")
def gold_fact_daily_compensation():
    closures = spark.read.table("silver_street_closures")
    businesses = spark.read.table("silver_businesses")

    # Filter closures overlapping the target year
    closures_yr = closures.filter(
        (F.col("closure_start") <= F.lit(YEAR_END))
        & (F.col("closure_end") >= F.lit(YEAR_START))
    )

    # Clamp to year boundaries
    closures_yr = closures_yr.withColumn(
        "effective_start",
        F.greatest(F.to_date("closure_start"), F.to_date(F.lit(YEAR_START))),
    ).withColumn(
        "effective_end",
        F.least(F.to_date("closure_end"), F.to_date(F.lit(YEAR_END))),
    )

    # Explode into daily rows
    closure_days = closures_yr.withColumn(
        "closure_date",
        F.explode(F.sequence(
            F.col("effective_start"), F.col("effective_end"),
            F.expr("INTERVAL 1 DAY")
        )),
    ).select("affected_street", "closure_date", "closure_id", "k_sug").distinct()

    # Distinct (street, date) to avoid multi-counting from overlapping closures
    street_days = closure_days.select("affected_street", "closure_date").distinct()

    # Join with businesses
    joined = street_days.join(
        businesses.select("id_esek", "shimush", "shem_rechov", "shetach"),
        street_days["affected_street"] == businesses["shem_rechov"],
        "inner",
    )

    # Deduplicate per (business, date) and compute compensation
    fact = joined.select(
        "id_esek", "shimush", "shem_rechov", "shetach", "closure_date"
    ).distinct()

    fact = fact.withColumn(
        "daily_compensation_ils",
        F.least(F.col("shetach") * 100, F.lit(10000)).cast("double"),
    )

    return fact
