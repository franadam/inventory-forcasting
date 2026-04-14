from itertools import chain

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.data_loading import read_postgresql_table
from spark.common.gold_utils import reorder_columns, add_surrogate_key

EN_PROVINCE_TO_REGION = {
    "Antwerp": "Flanders",
    "East Flanders": "Flanders",
    "Flemish Brabant": "Flanders",
    "Limburg": "Flanders",
    "West Flanders": "Flanders",
    "Hainaut": "Wallonia",
    "Liège": "Wallonia",
    "Luxembourg": "Wallonia",
    "Namur": "Wallonia",
    "Walloon Brabant": "Wallonia",
    "Brussels": "Brussels Capital Region",
}

NL_PROVINCE_TO_REGION = {
    "Antwerpen": "Flanders",
    "Oost-Vlaanderen": "Flanders",
    "Vlaams-Brabant": "Flanders",
    "Limburg": "Flanders",
    "West-Vlaanderen": "Flanders",
    "Henegouwen": "Wallonia",
    "Luik": "Wallonia",
    "Luxemburg": "Wallonia",
    "Namen": "Wallonia",
    "Waals-Brabant": "Wallonia",
    "Brussel": "Brussels Capital Region",
}

DE_PROVINCE_TO_REGION = {
    "Antwerpen": "Flanders",
    "Ostflandern": "Flanders",
    "Flämisch-Brabant": "Flanders",
    "Limburg": "Flanders",
    "Westflandern": "Flanders",
    "Hennegau": "Wallonia",
    "Lüttich": "Wallonia",
    "Luxemburg": "Wallonia",
    "Namür": "Wallonia",
    "Wallonisch-Brabant": "Wallonia",
    "Brüssel": "Brussels Capital Region",
}

FR_PROVINCE_TO_REGION = {
    "Anvers": "Flanders",
    "Flandre orientale": "Flanders",
    "Brabant flamand": "Flanders",
    "Limbourg": "Flanders",
    "Flandre occidentale": "Flanders",
    "Hainaut": "Wallonia",
    "Liège": "Wallonia",
    "Luxembourg": "Wallonia",
    "Namur": "Wallonia",
    "Brabant wallon": "Wallonia",
    "Bruxelles": "Brussels Capital Region",
}

PROVINCE_TO_REGION = {
    **EN_PROVINCE_TO_REGION,
    **NL_PROVINCE_TO_REGION,
    **DE_PROVINCE_TO_REGION,
    **FR_PROVINCE_TO_REGION,
}

def build_province_region_map() -> F.Column:
    mapping_expr = F.create_map(
        [F.lit(x) for x in chain(*PROVINCE_TO_REGION.items())]
    )
    return mapping_expr

def prepare_customer_locations(spark: SparkSession) -> DataFrame:
    customer_df = read_postgresql_table(spark=spark, schema="silver", table="customers")
    customer_locations_df = customer_df \
        .select("customer_code", "address", "city", "postal_code", "region")\
        .withColumn("location_type", F.lit("private"))\
        .withColumnRenamed("customer_code", "BK_dim_location")\
        .dropDuplicates()
    return customer_locations_df

def prepare_internal_locations(spark: SparkSession)  -> DataFrame:
    location_df = read_postgresql_table(spark=spark, schema="silver", table="locations")
    silver_location_df = location_df \
        .select("location_code", "location_type", "address", "city", "postal_code", "region")\
        .withColumnRenamed("location_code", "BK_dim_location")\
        .dropDuplicates()
    return silver_location_df

def enrich_location_geography(df: DataFrame, province_region_map: F.Column) -> DataFrame:
    enrich_df = df\
        .withColumnRenamed("region", "province") \
        .withColumn("region", province_region_map[F.col("province")])
    return enrich_df

def add_dim_location_surrogate_key(df: DataFrame) -> DataFrame:
    order_by_list = ["city", "postal_code", "address", "location_type", "BK_dim_location"]
    cleaned_df = add_surrogate_key(df=df, surrogate_key="PK_dim_location", ordered_list= order_by_list)
    return cleaned_df

def reorder_dim_location_columns(df: DataFrame) -> DataFrame:
    ordered_list = ["PK_dim_location", "BK_dim_location", "location_type", "address", "city", "postal_code", "province", "region"]
    ordered_df = reorder_columns(df=df, ordered_list=ordered_list)
    return ordered_df
