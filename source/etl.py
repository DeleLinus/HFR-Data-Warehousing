# import findspark
#
# # Get spark location on PC
# SPARK_HOME = os.getenv("SPARK_HOME")
# findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import IntegerType, StringType, ArrayType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
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


def read_and_transform_data(spark, input_data):
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
   """
    print("\nRunning read_and_transform_data")
    # get filepath to raw data file and read-in
    df = spark.read.csv(input_data, inferSchema=True, header=True)
    # select entries with 1 or more doctors
    df = df.filter(df.Number_of_Doctors > 0)

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
    # replace empty values in the form [] with null
    df = df.replace(to_replace="[]", value=None)
    # assign proper datatype
    df = df.withColumn("State_Unique_ID", col('State_Unique_ID').cast(IntegerType()))

    # prepare non atomic data for normalization (1NF)
    non_atomic_cols = ['Medical_Services', 'Surgical_Services', 'OG_Services',
                            'Pediatrics_Services', 'Dental_Services', 'SC_Services',
                            'Days_of_Operation']

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

    for non_atomic_col in non_atomic_cols:
        # make non-atomic value a list, coalesce helps to make null value into array here
        df = df.withColumn(non_atomic_col, f.coalesce(make_list(non_atomic_col), f.array()))

    # transform the mega data to conform to the schema used for dim_specialized_services table
    # convert the arrray columns to map, with the keys being the service type names/column name
    specialized_services_cols_dict = {'Medical_Services': 'Medical',
                                      'Surgical_Services': 'Surgical',
                                      'OG_Services': 'Obsterics and Gynecology',
                                      'Pediatrics_Services': 'Pediatrics',
                                      'Dental_Services': 'Dental',
                                      'SC_Services': 'Specific Clinical'}
    for key_v, value_v in specialized_services_cols_dict.items():
        df = df.withColumn(key_v, f.create_map(f.lit(value_v), key_v))


    # concat the specialized services mapped columns into one
    specialized_services_cols = list(specialized_services_cols_dict.keys())
    df = df.withColumn("Specialized_Services", f.map_concat(specialized_services_cols))

    # apply 1NF (create atomic values): explode_outer doesn't ignore null value like explode
    df = df.withColumn("Days", f.explode_outer(df["Days_of_Operation"]))
    df = df.select("*", f.explode_outer(df["Specialized_Services"]).alias("Service_Type", "Service_Name"))
    # Service_Name is now reduced to an array
    df = df.withColumn("Service_Name", f.explode_outer(df["Service_Name"]))

    # drop redundant columns
    df = df.drop(*specialized_services_cols)
    df = df.drop(*["Days_of_Operation", "Specialized_Services"])
    # replace empty strings (no longer necessary since I replaced values like []
    df = df.replace("", None)


    return df


def create_table_keys(mega_data=None, output_folder=None):
    """
    process_fact_personnel_table
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_data: pyspark dataframe
        This is the output dataframe from read_and_transform_data.
    output_folder: path
        This is the path that holds all saved files
    """



    dim_location_cols = ['State', 'State_Unique_ID', 'LGA', 'Ward',
                         'Physical_Location', 'Postal_Address', 'Longitude',
                         'Latitude']

    dim_contacts_cols = ['Phone_Number', 'Alternate_Number', 'Email_Address', 'Website']

    dim_facility_cols = ['Facility_Code', 'Facility_Name', 'Registration_No',
                         'Alternate_Name', 'Start_Date', 'Ownership', 'Ownership_Type',
                         'Facility_Level', 'Facility_Level_Option', 'Hours_of_Operation',
                         'Operational_Status', 'Registration_Status', 'License_Status']

    dim_operationalday_cols = ['Days']

    dim_commonservices_cols = ['Out_Patient_Services', 'In_Patient_Services', 'Onsite_Laboratory',
                               'Onsite_Imaging', 'Onsite_Pharmacy', 'Mortuary_Services', 'Ambulance_Services']

    dim_specializedservices_cols = ['Service_Name']

    tables_key_dict = {"Facility_Key": dim_facility_cols,
                       "Loc_Key": dim_location_cols,
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
        mega_data = mega_data.withColumn(key_v, f.dense_rank().over(window_spec))



    # write df to parquet file
    mega_data.write.parquet(os.path.join(output_folder, "doctors.parquet"), 'overwrite')
    print("doctors.parquet file created and saved in {}".format(output_folder))

    return mega_data

def process_fact_personnel_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    fact_cols = ['Facility_Key', 'Loc_Key', 'Contacts_Key', 'DO_Key', 'CS_Key', 'SS_Key', 'Total_number_of_Beds',
                 'Number_of_Doctors', 'Number_of_Pharmacists', 'Number_of_PT', 'Number_of_Dentists', 'Number_of_DT',
                 'Number_of_Nurses', 'Number_of_Midwifes', 'Number_of_N/M', 'Number_of_LT', 'Number_of_LS',
                 'HIM_Officers', 'Number_of_CHO', 'Number_of_CHEW', 'Number_of_JCHEW', 'Number_of_EHO', 'Number_of_HA']

    fact_personnel_table = mega_df.select(fact_cols).dropDuplicates().orderBy("Facility_Key")
    # log
    print("processed fact_personnel_table")
    return fact_personnel_table

def process_dim_facility_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    dim_facility_cols = ['Facility_Key', 'Facility_Name', 'Facility_Code', 'Registration_No',
                         'Alternate_Name', 'Start_Date', 'Ownership', 'Ownership_Type',
                         'Facility_Level', 'Facility_Level_Option', 'Hours_of_Operation',
                         'Operational_Status', 'Registration_Status', 'License_Status']

    dim_facility_table = mega_df.select(dim_facility_cols).dropDuplicates(["Facility_Key"]).orderBy("Facility_Key")
    # log
    print("processed dim_facility_table")
    return dim_facility_table

def process_dim_location_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    dim_location_cols = ['Loc_Key', 'State', 'State_Unique_ID', 'LGA', 'Ward',
                         'Physical_Location', 'Postal_Address', 'Longitude',
                         'Latitude']

    dim_location_table = mega_df.select(dim_location_cols).dropDuplicates(["Loc_Key"]).orderBy("Loc_Key")
    # log
    print("processed dim_location_table")
    return dim_location_table


def process_dim_contacts_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    dim_contacts_cols = ['Contacts_Key', 'Phone_Number', 'Alternate_Number', 'Email_Address', 'Website']

    dim_contacts_table = mega_df.select(dim_contacts_cols).dropDuplicates(["Contacts_Key"]).orderBy("Contacts_Key")
    # log
    print("processed dim_contacts_table")
    return dim_contacts_table



def process_dim_days_of_operation_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    dim_days_of_operation_cols = ['DO_Key', 'Days']

    dim_days_of_operation_table = mega_df.select(dim_days_of_operation_cols).dropDuplicates(["DO_Key"]).orderBy("DO_Key")
    # log
    print("processed dim_days_of_operation_table")
    return dim_days_of_operation_table


def process_dim_commonservices_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    dim_commonservices_cols = ['CS_Key', 'Out_Patient_Services', 'In_Patient_Services', 'Onsite_Laboratory',
                               'Onsite_Imaging', 'Onsite_Pharmacy', 'Mortuary_Services', 'Ambulance_Services']

    dim_commonservices_table = mega_df.select(dim_commonservices_cols).dropDuplicates(["CS_Key"]).orderBy("CS_Key")
    # log
    print("processed dim_commonservices_table")
    return dim_commonservices_table


def process_dim_specializedservices_table(mega_df):
    """
    This helps create Key-columns that would later be used as PK for the tables

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    dim_specializedservices_cols = ['SS_Key', 'Service_Name', 'Service_Type']

    dim_specializedservices_table = mega_df.select(dim_specializedservices_cols).dropDuplicates(["SS_Key"])\
                                                                                .orderBy("SS_Key")
    # log
    print("processed dim_specializedservices_table")
    return dim_specializedservices_table


def load_into_db():
    pass

if __name__ == '__main__':
    input_data = "raw_hfr_data.csv"
    output_folder1 = "output_parquet_folder"
    spark = create_spark_session()
    mega_data = read_and_transform_data(spark, input_data)
    mega_df = create_table_keys(mega_data=mega_data, output_folder=output_folder1)
    # tables
    table_fact = process_fact_personnel_table(mega_df)
    table_facility = process_dim_facility_table(mega_df)
    table_loc = process_dim_location_table(mega_df)
    table_contact = process_dim_contacts_table(mega_df)
    table_day = process_dim_days_of_operation_table(mega_df)
    table_cs = process_dim_commonservices_table(mega_df)
    table_ss = process_dim_specializedservices_table(mega_df)