import os
import findspark

# Get spark location on PC
SPARK_HOME = os.getenv("SPARK_HOME")
findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, row_number, lit
from pyspark.sql.types import IntegerType,  StringType, ArrayType
from pyspark.sql.window import Window




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
    This helps clean, normalize and generally perform
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

    # non-atomic column names dictionary
    non_atomic_cols_dict = {'Medical_Services': 'Medical_Services_nonatomic',
                            'Surgical_Services': 'Surgical_Services_nonatomic',
                            'OG_Services': 'OG_Services_nonatomic',
                            'Pediatrics_Services': 'Pediatrics_Services_nonatomic',
                            'Dental_Services': 'Dental_Services_nonatomic',
                            'SC_Services': 'SC_Services_nonatomic',
                            'Days_of_Operation': 'Days_of_Operation_nonatomic'}

    @udf(returnType=ArrayType(StringType()))
    def make_list(row):
        """
        makes non-atomic values into list
        """
        try:
            if "[" in str(row):
                # new value list for OG_Services and co with []
                nvl = row[2:-2].split("', '")
                return nvl
            else:
                # for Day of Operations column
                nvl = [str(i).strip() for i in row.split(',')]
                return nvl
        except:
            return row

    for key_v, value_v in non_atomic_cols_dict.items():
        # make non-atomic value a list
        df = df.withColumn(key_v, make_list(key_v))

    for key_v, value_v in non_atomic_cols_dict.items():
        # rename non-atomic columns to aid recognition after transformation
        df = df.withColumnRenamed(key_v, value_v)

    for key_v, value_v in non_atomic_cols_dict.items():
        # apply 1NF (create atomic values)
        df = df.select("*", explode(df[value_v]).alias(key_v))

    # drop non-atomic columns
    col_to_drop = list(non_atomic_cols_dict.values())
    df = df.drop(*col_to_drop)

    # write df to parquet file
    df.write.parquet(output_data)

    return df







if __name__ == '__main__':
    input_data = "raw_hfr_data.csv"
    output_data = "doctors.parquet"