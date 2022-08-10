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
import configparser

# read database credentials
config = configparser.ConfigParser()
config.read("warehouse_config.cfg")

PSQL_HOSTNAME = config['DATABASE']['PSQL_HOSTNAME']
PSQL_PORTNUMBER = config['DATABASE']['PSQL_PORTNUMBER']
PSQL_DBNAME = config['DATABASE']['PSQL_DBNAME']
PSQL_USERNAME = config['DATABASE']['PSQL_USERNAME']
PSQL_PASSWORD = config['DATABASE']['PSQL_PASSWORD']


def create_spark_session():
    """
    create sparkSession object
    https://stackoverflow.com/questions/34948296/using-pyspark-to-connect-to-postgresql
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars", "/C:/pyspark/spark-3.3.0-bin-hadoop2/jars/postgresql-42.2.26.jar") \
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

    @udf(returnType=StringType())
    def correct_phone_number(x):
        """
        cleans the phone_number column
        """
        try:
            if "-" in x:
                return x

            else:
                new_x = str(x[:4]) + "-" + str(x[4:7]) + "-" + str(x[7:])
                return new_x
        except:
            return x

    df = df.withColumn("phone_number", correct_phone_number("phone_number"))
    # replace invalid values with null
    df = df.replace(["LAGOS", "+23401"], [None, None], ['state_unique_ID', 'postal_address'])
    # replace empty values in the form [] with null
    df = df.replace(to_replace="[]", value=None)
    # assign proper datatype
    df = df.withColumn("state_unique_ID", col('state_unique_ID').cast(IntegerType()))

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
    df = df.withColumn("day", f.explode_outer(df["Days_of_Operation"]))
    df = df.select("*", f.explode_outer(df["Specialized_Services"]).alias("service_type", "service_name"))
    # service_name is now reduced to an array
    df = df.withColumn("service_name", f.explode_outer(df["service_name"]))

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



    dim_location_cols = ['state', 'state_unique_ID', 'LGA', 'ward',
                         'physical_location', 'postal_address', 'longitude',
                         'latitude']

    dim_contacts_cols = ['phone_number', 'alternate_number', 'email_address', 'website']

    dim_facility_cols = ['facility_code', 'facility_name', 'registration_no',
                         'alternate_name', 'start_date', 'ownership', 'ownership_type',
                         'facility_level', 'facility_level_option', 'hours_of_operation',
                         'operation_status', 'registration_status', 'license_status']

    dim_operationalday_cols = ['day']

    dim_commonservices_cols = ['out_patient_services', 'in_patient_services', 'onsite_laboratory',
                               'onsite_imaging', 'onsite_pharmacy', 'mortuary_services', 'ambulance_services']

    dim_specializedservices_cols = ['service_name']

    tables_key_dict = {"institution_key": dim_facility_cols,
                       "loc_key": dim_location_cols,
                       "contacts_key": dim_contacts_cols,
                       "DO_key": dim_operationalday_cols,
                       "CS_key": dim_commonservices_cols,
                       "SS_key": dim_specializedservices_cols
                       }

    """
    I could do away with partitionBy but according to 
    https://stackoverflow.com/questions/33102727/primary-keys-with-apache-spark
    It would means all data are moved to a single partition
    which can cause serious performance degradation.

    Also, dense_rank is used instead of rank because it doesn't leave gaps
    https://sparkbyexamples.com/pyspark/pyspark-window-functions/#dense_rank
    """
    for key_v, value_v in tables_key_dict.items():
        window_spec = Window.partitionBy(lit("A")).orderBy(value_v)
        mega_data = mega_data.withColumn(key_v, f.dense_rank().over(window_spec))

    # select entries with 1 or more doctors and write to parquet file
    doctors_df = mega_data.filter(mega_data.number_of_doctors > 0)
    doctors_df.write.parquet(os.path.join(output_folder, "doctors.parquet"), 'overwrite')
    print("doctors.parquet file created and saved in {}".format(output_folder))

    return mega_data

def process_tables(mega_df):
    """
    This helps create tables with reference to the ERD and Schema I have proposed in the
    database model

    Parameters
    ----------
    mega_df: pyspark dataframe
        This is the output dataframe from create_table_keys.
    """
    fact_cols = ['institution_key', 'loc_key', 'contacts_key', 'DO_key', 'CS_key', 'SS_key', 'total_number_of_beds',
                 'number_of_doctors', 'number_of_pharmacists', 'number_of_PT', 'number_of_dentists', 'number_of_DT',
                 'number_of_nurses', 'number_of_midwifes', 'number_of_N/M', 'number_of_LT', 'number_of_LS',
                 'number_of_HIMO', 'number_of_CHO', 'number_of_CHEW', 'number_of_JCHEW', 'number_of_EHO', 'number_of_HA']
    dim_facility_cols = ['institution_key', 'facility_name', 'facility_code', 'registration_no',
                         'alternate_name', 'start_date', 'ownership', 'ownership_type',
                         'facility_level', 'facility_level_option', 'hours_of_operation',
                         'operation_status', 'registration_status', 'license_status']
    dim_location_cols = ['loc_key', 'state', 'state_unique_ID', 'LGA', 'ward',
                         'physical_location', 'postal_address', 'longitude',
                         'latitude']
    dim_contacts_cols = ['contacts_key', 'phone_number', 'alternate_number', 'email_address', 'website']
    dim_day_of_operation_cols = ['DO_key', 'day']
    dim_commonservices_cols = ['CS_key', 'out_patient_services', 'in_patient_services', 'onsite_laboratory',
                               'onsite_imaging', 'onsite_pharmacy', 'mortuary_services', 'ambulance_services']
    dim_specializedservices_cols = ['SS_key', 'service_name', 'Service_Type']

    # select columns and log
    fact_personnel_table = mega_df.select(fact_cols).dropDuplicates().orderBy("institution_key")
    print("processed fact_personnel_table")
    dim_facility_table = mega_df.select(dim_facility_cols).dropDuplicates(["institution_key"]).orderBy("institution_key")
    print("processed dim_facility_table")
    dim_location_table = mega_df.select(dim_location_cols).dropDuplicates(["loc_key"]).orderBy("loc_key")
    print("processed dim_location_table")
    dim_contacts_table = mega_df.select(dim_contacts_cols).dropDuplicates(["contacts_key"]).orderBy("contacts_key")
    print("processed dim_contacts_table")
    dim_day_of_operation_table = mega_df.select(dim_day_of_operation_cols).dropDuplicates(["DO_key"]).orderBy("DO_key")
    print("processed dim_day_of_operation_table")
    dim_commonservices_table = mega_df.select(dim_commonservices_cols).dropDuplicates(["CS_key"]).orderBy("CS_key")
    print("processed dim_commonservices_table")
    dim_specializedservices_table = mega_df.select(dim_specializedservices_cols).dropDuplicates(["SS_key"])
    print("processed dim_specializedservices_table")

    return (fact_personnel_table, dim_facility_table, dim_location_table, dim_contacts_table, dim_day_of_operation_table,
            dim_commonservices_table, dim_specializedservices_table)


def main():
    """
    Perform the following roles:
    1. Get or create a spark session.
    2. Read the data output from scraper.py
    3. take the data and transform them to tables
    which will then save the mega data to parquet file.
    4. Load the tables into PostgreSQL.
    """
    input_data = "raw_hfr_data.csv"
    output_folder1 = "output_parquet_folder"
    spark = create_spark_session()
    # just to off logging (default is "WARN")
    spark.sparkContext.setLogLevel("OFF")
    mega_df = create_table_keys(mega_data=read_and_transform_data(spark, input_data), output_folder=output_folder1)
    # tables
    table_fact, table_facility, table_loc, table_contact, table_day, table_cs, table_ss = process_tables(mega_df)

    tables_dict = {"factPersonnel": table_fact, "dimInstitutions": table_facility,
                   "dimLocation": table_loc, "dimContacts": table_contact,
                   "dimOperationalDay": table_day, "dimCommonServices": table_cs,
                   "dimSpecializedServices": table_ss
                   }

    URL = f"jdbc:postgresql://{PSQL_HOSTNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
    for table_name, table_data in tables_dict.items():
        table_data.write\
                  .format("jdbc")\
                  .option("url", URL) \
                  .option("dbtable", table_name) \
                  .option("user", PSQL_USERNAME) \
                  .option("password", PSQL_PASSWORD) \
                  .option("driver", 'org.postgresql.Driver') \
                  .mode("overwrite") \
                  .save()
        print("{0} table loaded into {1} database".format(table_name, PSQL_DBNAME))


if __name__ == '__main__':
    main()