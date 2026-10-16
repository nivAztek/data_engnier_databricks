"""Silver: Street reference dimension - all known streets from both sources."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="silver_dim_streets",
    comment="Street reference dimension with normalized names from closures and businesses.",
)
def silver_dim_streets():
    # Streets from closures (both columns)
    closures = spark.read.table("silver_street_closures")
    closure_streets = closures.select(
        F.col("affected_street").alias("street_name")
    ).distinct()

    # Streets from businesses
    businesses = spark.read.table("silver_businesses")
    biz_streets = businesses.select(
        F.trim(F.col("shem_rechov")).alias("street_name")
    ).distinct()

    # Union and deduplicate
    all_streets = closure_streets.union(biz_streets).distinct()
    all_streets = all_streets.filter(
        F.col("street_name").isNotNull() & (F.col("street_name") != "")
    )

    # Add source flags
    all_streets = all_streets.join(
        closure_streets.withColumn("in_closures", F.lit(True)),
        "street_name", "left"
    ).join(
        biz_streets.withColumn("in_businesses", F.lit(True)),
        "street_name", "left"
    ).fillna(False, ["in_closures", "in_businesses"])

    return all_streets.withColumn(
        "street_id", F.monotonically_increasing_id()
    ).select("street_id", "street_name", "in_closures", "in_businesses")
