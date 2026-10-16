"""Bronze: Ingest street closures via Auto Loader from UC Volume."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

VOLUME_PATH = spark.conf.get("pipeline.volume_path", "/Volumes/main/tel_aviv_dev/raw_data")


@dp.table(
    name="bronze_street_closures",
    comment="Raw street closure events ingested incrementally via Auto Loader.",
)
@dp.expect_or_drop("valid_id", "ID IS NOT NULL")
@dp.expect_or_drop("valid_street_1", "me_shem_rechov IS NOT NULL")
@dp.expect_or_drop("valid_street_2", "ad_shem_rechov IS NOT NULL")
@dp.expect_or_drop("valid_start_date", "tr_from IS NOT NULL")
def bronze_street_closures():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{VOLUME_PATH}/street_closures/")
    )
