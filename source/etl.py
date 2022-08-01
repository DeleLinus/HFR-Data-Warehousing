import os
import findspark

# Get spark location on PC
SPARK_HOME = os.getenv("SPARK_HOME")
findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, asc, row_number, lit
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.types import MapType, StructType, StructField , DoubleType, IntegerType ,  StringType, TimestampType
from pyspark.sql.window import Window

import numpy as np
import pandas as pd


def create_spark_session():
    '''
    create sparkSession object
    '''
    spark = SparkSession \
        .builder \
        .appName("HFR Data Pipeline") \
        .getOrCreate()
    return spark


def read_and_transform_data(spark, input_data, output_data):
    '''
        """
    This help clean, normalize and generally perform
    adequate transformation on the hfr data to have it
    as a parquet file that can be loaded into the final destination
    or further degenerated into different fact and dimension tables

    Parameters
    ----------
    spark: session
        This is the spark session that has been created
    input_data: path
        This is the path to the raw hfr data saved by the scraper.
    output_data: path
        This is the path that holds all saved files

   '''
    print("\nRunning read_and_transform_data")
