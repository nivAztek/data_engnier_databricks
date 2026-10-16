"""Bronze: Ingest businesses via Auto Loader from UC Volume (JSON snapshots)."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType
)

spark = SparkSession.builder.getOrCreate()

VOLUME_PATH = spark.conf.get("pipeline.volume_path", "/Volumes/main/tel_aviv_dev/raw_data")

# Schema for a single feature element (multiLine=true reads each array element as a row)
FEATURE_SCHEMA = StructType([
    StructField("attributes", StructType([
        StructField("OBJECTID", IntegerType(), True),
        StructField("id_esek", IntegerType(), True),
        StructField("shimush", StringType(), True),
        StructField("shem_rechov", StringType(), True),
        StructField("ms_bayit", StringType(), True),
        StructField("tr_hakama", LongType(), True),
        StructField("sw_taun_rishui", IntegerType(), True),
        StructField("shetach", IntegerType(), True),
        StructField("x_coord", DoubleType(), True),
        StructField("y_coord", DoubleType(), True),
        StructField("date_import", StringType(), True),
        StructField("shem_machzik_rashi", StringType(), True),
        StructField("ms_mezahe_machzik_rashi", StringType(), True),
    ]), True),
    StructField("geometry", StructType([
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
    ]), True),
])


@dp.table(
    name="bronze_businesses",
    comment="Raw business records ingested incrementally via Auto Loader.",
)
@dp.expect_or_drop("valid_id_esek", "id_esek IS NOT NULL")
@dp.expect_or_drop("valid_street", "shem_rechov IS NOT NULL")
def bronze_businesses():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .schema(FEATURE_SCHEMA)
        .load(f"{VOLUME_PATH}/businesses/")
    )
    # multiLine=true reads each array element as a row; flatten nested structs
    return raw.select(
        F.col("attributes.OBJECTID").alias("OBJECTID"),
        F.col("attributes.id_esek").alias("id_esek"),
        F.col("attributes.shimush").alias("shimush"),
        F.col("attributes.shem_rechov").alias("shem_rechov"),
        F.col("attributes.ms_bayit").alias("ms_bayit"),
        F.col("attributes.tr_hakama").alias("tr_hakama"),
        F.col("attributes.sw_taun_rishui").alias("sw_taun_rishui"),
        F.col("attributes.shetach").alias("shetach"),
        F.col("attributes.x_coord").alias("x_coord"),
        F.col("attributes.y_coord").alias("y_coord"),
        F.col("attributes.date_import").alias("date_import"),
        F.col("attributes.shem_machzik_rashi").alias("shem_machzik_rashi"),
        F.col("attributes.ms_mezahe_machzik_rashi").alias("ms_mezahe_machzik_rashi"),
        F.col("geometry.x").alias("geom_x"),
        F.col("geometry.y").alias("geom_y"),
        F.current_timestamp().alias("_ingested_at"),
    )
