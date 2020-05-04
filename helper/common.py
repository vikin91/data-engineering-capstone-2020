"""
This script contains helper functions commonly used in the other helpers.
"""

from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when

# Set the date format to YYYY-MM-DD
DATE_FORMAT = "%Y-%m-%d"


def create_spark_session():
    """
    This function creates a session with Spark.

    Returns:
        spark (:obj:`SparkSession`): Spark session handle
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def print_data(df, limit=10):
    """
    Convert dataframe to pandas and display nicely-formatted head.

    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        limit (:obj: `int`): number of the rows from the head to be presented
    Returns:
        string: String representation of the dataframe
    """

    print("Num rows = ", df.count())
    return df.limit(limit).toPandas().head(limit)

def rename_field(df, field_name, change_list):
    """
    Rename column values based on condition.

    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        field_name (:obj: `string`): name of the field for its contents to be renamed
        change_list (:obj: `list`): List of tuples in the format (old value, new value)
    Returns:
        df (:obj:`SparkDataFrame`): Processed Spark dataframe
    """
    for old, new in change_list:
        df = df.withColumn(field_name, when(
            df[field_name] == old, new).otherwise(df[field_name]))
    return df


def convert_to_integer(df, cols):
    """
    Convert the column to integer

    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        cols (:obj:`list`): List of column names that should be converted to integer
    Returns:
        df (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    for col in [col for col in cols if col in df.columns]:
        df = df.withColumn(col, df[col].cast("integer"))
    return df


def convert_to_float(df, cols):
    """
    Convert the columns to float

    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        cols (:obj:`list`): List of column names that should be converted to float
    Returns:
        df (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    for col in [col for col in cols if col in df.columns]:
        df = df.withColumn(col, df[col].cast("float"))
    return df


def convert_to_date(df, cols):
    """
    Convert the columns to date

    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        cols (:obj:`list`): List of column names that should be converted to date
    Returns:
        df (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    for col in [col for col in cols if col in df.columns]:
        df = df.withColumn(col, CONVERT_SAS_UDF(df[col]))
    return df


# Converts SAS dates into string dates in the format YYYY-MM-DD
CONVERT_SAS_UDF = udf(lambda x: x if x is None else (
    timedelta(days=x) + datetime(1960, 1, 1)).strftime(DATE_FORMAT))
