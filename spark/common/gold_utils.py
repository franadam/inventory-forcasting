from itertools import chain

from pyspark.sql import DataFrame, Window, Column
from pyspark.sql import functions as F

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


def add_surrogate_key(df: DataFrame, surrogate_key: str, ordered_list: list) -> DataFrame:
    window_spec = Window.orderBy(*ordered_list)
    cleaned_df = df.withColumn(surrogate_key, F.row_number().over(window_spec))
    return cleaned_df


def reorder_columns(df: DataFrame, ordered_list: list) -> DataFrame:
    ordered_df = df.select(*ordered_list)
    return ordered_df


def build_province_region_map() -> F.Column:
    mapping_expr = F.create_map(
        [F.lit(x) for x in chain(*PROVINCE_TO_REGION.items())]
    )
    return mapping_expr


def enrich_location_geography(df: DataFrame, province_region_map: F.Column) -> DataFrame:
    enrich_df = df\
        .withColumnRenamed("region", "province") \
        .withColumn("region", province_region_map[F.col("province")])
    return enrich_df


def generate_code_helper(group: Column, group_id: Column):
    group_code = F.upper(F.substring(group, 0, 3))
    return F.concat_ws("-", group_code, group_id)
