"""Silver: Businesses dimension - SCD Type 1 via Auto CDC.
Deduplicates by id_esek, keeping latest by OBJECTID.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Create the target streaming table for CDC
dp.create_streaming_table(
    name="silver_businesses",
    comment="Business dimension (SCD Type 1). Latest state per id_esek.",
    cluster_by=["shem_rechov"],
    expect_all_or_drop={
        "valid_id_esek": "id_esek IS NOT NULL",
        "valid_shetach": "shetach >= 0",
    },
)

# Apply CDC flow: upsert by id_esek, sequenced by OBJECTID
dp.create_auto_cdc_flow(
    target="silver_businesses",
    source="bronze_businesses",
    keys=["id_esek"],
    sequence_by="OBJECTID",
    stored_as_scd_type="1",
    column_list=[
        "id_esek",
        "shimush",
        "shem_rechov",
        "ms_bayit",
        "tr_hakama",
        "shetach",
        "x_coord",
        "y_coord",
        "geom_x",
        "geom_y",
        "date_import",
        "shem_machzik_rashi",
        "OBJECTID",
    ],
)
