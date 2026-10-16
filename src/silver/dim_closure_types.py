"""Silver: Closure type reference dimension."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="silver_dim_closure_types",
    comment="Reference dimension for closure type codes (k_sug / t_sug).",
)
def silver_dim_closure_types():
    closures = spark.read.table("silver_street_closures")
    return (
        closures.select("k_sug", "t_sug")
        .distinct()
        .filter(F.col("k_sug").isNotNull())
        .orderBy("k_sug")
    )
