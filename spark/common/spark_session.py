from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("inventory_forcasting")\
    .getOrCreate()
