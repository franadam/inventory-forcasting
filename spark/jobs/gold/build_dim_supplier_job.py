import logging

from pyspark.sql import DataFrame

from spark.common.spark_session import build_spark_session
from spark.common.dataframe_utils import save_into_db
from spark.transformations.gold.dim_supplier import prepare_internal_suppliers, reorder_dim_supplier_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_dim_supplier() -> DataFrame:
    spark = build_spark_session("build_dim_supplier")

    supplier_df = prepare_internal_suppliers(spark)

    dim_supplier_df = supplier_df\
        .transform(reorder_dim_supplier_columns)

    save_into_db(schema='gold', table='dim_supplier',
                 dataframe=dim_supplier_df, mode="append")

    return dim_supplier_df


if __name__ == "__main__":
    build_dim_supplier()
