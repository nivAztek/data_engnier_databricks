"""Shared pipeline configuration. Reads from pipeline spark.conf at runtime."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Pipeline configuration with defaults
YEAR_START = spark.conf.get("pipeline.year_start", "2023-01-01")
YEAR_END = spark.conf.get("pipeline.year_end", "2023-12-31")
VOLUME_PATH = spark.conf.get("pipeline.volume_path", "/Volumes/main/tel_aviv_dev/raw_data")
CSV_URL = "https://storage.googleapis.com/test_onedatai/rechov_sagur.csv"
API_BASE_URL = "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query"
API_BATCH_SIZE = 2000
