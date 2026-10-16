"""Silver: Cleaned street closures - parsed dates, exploded per street.
Streaming table for append-only event data.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()


@dp.table(
    name="silver_street_closures",
    comment="Cleaned closures with parsed dates and one row per affected street.",
    cluster_by=["affected_street", "closure_start"],
)
@dp.expect_or_fail("valid_closure_start", "closure_start IS NOT NULL")
@dp.expect_or_fail("valid_affected_street", "affected_street IS NOT NULL AND affected_street != \'\'")
def silver_street_closures():
    df = spark.readStream.table("bronze_street_closures")

    # Parse dates (DD/MM/YYYY H:MM)
    df = df.withColumn(
        "closure_start", F.to_timestamp(F.col("tr_from"), "dd/MM/yyyy H:mm")
    ).withColumn(
        "closure_end_raw", F.to_timestamp(F.col("tr_to"), "dd/MM/yyyy H:mm")
    ).withColumn(
        "closure_end", F.coalesce(F.col("closure_end_raw"), F.col("closure_start"))
    )

    # Normalize and explode both streets
    df = df.withColumn("me_shem_rechov", F.trim(F.col("me_shem_rechov")))
    df = df.withColumn("ad_shem_rechov", F.trim(F.col("ad_shem_rechov")))
    df = df.withColumn(
        "affected_street",
        F.explode(F.array(F.col("me_shem_rechov"), F.col("ad_shem_rechov")))
    )
    df = df.filter(
        F.col("affected_street").isNotNull()
        & (F.trim(F.col("affected_street")) != "")
    )

    return df.select(
        F.col("ID").alias("closure_id"),
        "id_rechov",
        "affected_street",
        "closure_start",
        "closure_end",
        "k_sug",
        "t_sug",
        "k_sgira",
        "shaot",
        "me_shem_rechov",
        "ad_shem_rechov",
    )
