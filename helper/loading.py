"""
This script contains helper functions used for reading the data from files.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def read_csv(spark, filepath, separator=","):
    """
    Loads data into Spark Session from CSV file.

    Args:
        spark (:obj:`SparkSession`): Spark session handle
        filepath (:string:): Path to the CSV file to load
        separator (:string:): Field separator in CSV, default - ','
    Returns:
        (:obj:`SparkDataFrame`): Spark dataframe
    """

    return spark.read.format('csv').option("header", "true").option("sep", separator).load(filepath)


def read_json(spark, filepath):
    """
    Loads data into Spark Session from JSON file.

    Args:
        spark (:obj:`SparkSession`): Spark session handle
        filepath (:string:): Path to the JSON file to load
    Returns:
        (:obj:`SparkDataFrame`): Spark dataframe
    """

    return spark.read.json(filepath)


def load_airlines(spark, filepath):
    """
    Loads data into Spark Session from a airlines-dataset-specific dat file.

    Args:
        spark (:obj:`SparkSession`): Spark session handle
        filepath (:string:): Path to the 'dat' file to load
    Returns:
        (:obj:`SparkDataFrame`): Spark dataframe
    """

    schema = StructType([
        StructField("Airline_ID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("IATA", StringType(), True),
        StructField("ICAO", StringType(), True),
        StructField("Callsign", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Active", StringType(), True)])

    return spark.read.csv(filepath, header=False, schema=schema)
