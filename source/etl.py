# import findspark
#
# # Get spark location on PC
# SPARK_HOME = os.getenv("SPARK_HOME")
# findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, row_number, lit, dense_rank
from pyspark.sql.types import IntegerType, StringType, ArrayType
from pyspark.sql.window import Window
import os


def create_spark_session():
    """
    create sparkSession object
    """
    spark = SparkSession \
        .builder \
        .appName("HFR Data Pipeline") \
        .getOrCreate()
    return spark


def read_and_transform_data(spark, input_data, output_folder):
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
    output_folder: path
        This is the path that holds all saved files
   """
    print("\nRunning read_and_transform_data")
    # get filepath to raw data file and read-in
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
    cols_to_drop = list(non_atomic_cols_dict.values())
    df = df.drop(*cols_to_drop)
    # replace empty strings
    df = df.replace("", None)

    # create PK/FK columns
    fact_cols = ['Facility_Key', 'Loc_Key', 'Contacts_Key', 'DO_Key', 'CS_Key', 'SS_Key', 'Total_number_of_Beds',
                 'Number_of_Doctors', 'Number_of_Pharmacists', 'Number_of_PT', 'Number_of_Dentists', 'Number_of_DT',
                 'Number_of_Nurses', 'Number_of_Midwifes', 'Number_of_N/M', 'Number_of_LT', 'Number_of_LS',
                 'HIM_Officers', 'Number_of_CHO', 'Number_of_CHEW', 'Number_of_JCHEW', 'Number_of_EHO', 'Number_of_HA']

    dim_location_cols = ['State', 'State_Unique_ID', 'LGA', 'Ward',
                         'Physical_Location', 'Postal_Address', 'Longitude',
                         'Latitude']

    dim_contacts_cols = ['Phone_Number', 'Alternate_Number', 'Email_Address', 'Website']

    dim_facility_cols = ['Facility_Key', 'Facility_Code', 'Facility_Name', 'Registration_No',
                         'Alternate_Name', 'Start_Date', 'Ownership', 'Ownership_Type',
                         'Facility_Level', 'Facility_Level_Option', 'Hours_of_Operation',
                         'Operational_Status', 'Registration_Status', 'License_Status']

    dim_operationalday_cols = ['Days_of_Operation']

    dim_commonservices_cols = ['Out_Patient_Services', 'In_Patient_Services', 'Onsite_Laboratory',
                               'Onsite_Imaging', 'Onsite_Pharmacy', 'Mortuary_Services', 'Ambulance_Services']

    dim_specializedservices_cols = ['Medical_Services', 'Surgical_Services', 'OG_Services',
                                    'Pediatrics_Services', 'Dental_Services', 'SC_Services']

    tables_key_dict = {"Loc_Key": dim_location_cols,
                       "Contacts_Key": dim_contacts_cols,
                       "DO_Key": dim_operationalday_cols,
                       "CS_Key": dim_commonservices_cols,
                       "SS_Key": dim_specializedservices_cols
                       }

    """
    I could do away with partitionBy but according to 
    https://stackoverflow.com/questions/33102727/primary-keys-with-apache-spark
    It would means all data are moved to a single partition
    which can cause serious performance degradation.

    Also, dense_key is used instead of rank because it doesn't leave gaps
    https://sparkbyexamples.com/pyspark/pyspark-window-functions/#dense_rank
    """
    for key_v, value_v in tables_key_dict.items():
        window_spec = Window.partitionBy(lit("A")).orderBy(value_v)
        df = df.withColumn(key_v, dense_rank().over(window_spec))



    # write df to parquet file
    df.write.parquet(os.path.join(output_folder, "doctors.parquet"), 'overwrite')
    print("doctors.parquet file created and saved in {}".format(output_folder))
    return df


def process_fact_personnel_table(mega_data):
    """
    This helps process the mega data into the fact table

    Parameters
    ----------
    mega_data: pyspark dataframe
        This is the output dataframe from read_and_transform_data.
    """


if __name__ == '__main__':
    input_data = "raw_hfr_data.csv"
    output_folder = "output_parquet_folder"
    spark = create_spark_session()
    read_and_transform_data(spark, input_data, output_folder)
