import os
import findspark

# Get spark location on PC
SPARK_HOME = os.getenv("SPARK_HOME")
findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, asc, row_number, lit
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.types import MapType, StructType, StructField, DoubleType, IntegerType,  StringType, ArrayType
from pyspark.sql.window import Window

import numpy as np
import pandas as pd


def create_spark_session():
    """
    create sparkSession object
    """
    spark = SparkSession \
        .builder \
        .appName("HFR Data Pipeline") \
        .getOrCreate()
    return spark


def read_and_transform_data(spark, input_data, output_data):
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
   """
    print("\nRunning read_and_transform_data")
    # read-in the data
    df = spark.read.csv(input_data, inferSchema=True, header=True)
    # select entries with 1 or more doctors
    df = df.filter(df.Number_of_Doctors > 0)
    # add facility Key column
    w = Window().orderBy(lit('A'))
    df = df.withColumn("Facility_Key", row_number().over(w))

    @udf(returnType=StringType())
    def correct_phone_number(x):
        """
        cleans the Phone_Number column
        """
        try:
            if "-" in x:
                return x

            else:
                new_x = str(x[:4]) + "-" + str(x[4:7]) + "-" + str(x[7:])
                return new_x
        except:
            return x

    df = df.withColumn("Phone_Number", correct_phone_number("Phone_Number"))
    # replace invalid values with null
    df = df.replace(["LAGOS", "+23401"], [None, None], ['State_Unique_ID', 'Postal_Address'])
    # assign proper datatype
    df = df.withColumn("State_Unique_ID", col('State_Unique_ID').cast(IntegerType()))

    # prepare non atomic data for normalization (1NF)
    make_list = udf(lambda x: x[2:-2].split("', '"), returnType=ArrayType(StringType()))
    df = df \
        .withColumn('Medical_Services', make_list('Medical_Services')) \
        .withColumn('Surgical_Services', make_list('Surgical_Services')) \
        .withColumn('OG_Services', make_list('OG_Services')) \
        .withColumn('Pediatrics_Services', make_list('Pediatrics_Services')) \
        .withColumn('Dental_Services', make_list('Dental_Services')) \
        .withColumn('SC_Services', make_list('SC_Services'))