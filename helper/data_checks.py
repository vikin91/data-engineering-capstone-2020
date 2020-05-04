"""
This script contains helper functions used for checking data quality.
"""

from pyspark.sql.functions import col


def has_more_rows(dataframe, num_rows=0):
    """
    Checks whether the datafram contains more rows than `num_rows`.

    Args:
        dataframe (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        num_rows (:obj:`int`): Threshold for the number of rows
    Returns:
        bool: True if check is passed - there are more rows than the threshold - false otherwise.
    """
    return dataframe.count() > num_rows


def check_integrity(fact, dim_demographics, dim_airports, dim_airlines, dim_countries):
    """
    Checks integrity of the model.
    Checks if all facts columns joined with the dimension tables have correct values.

    Args:
        immigration (:obj:`SparkDataFrame`): Spark dataframe with temperature dataset
        dim_demographics (:obj:`SparkDataFrame`): Spark dataframe with demographics dataset
        dim_airlines (:obj:`SparkDataFrame`): Spark dataframe with airlines dataset
        dim_countries (:obj:`SparkDataFrame`): Spark dataframe with countries dataset
    Returns:
        bool: True if check is passed - all data is integral - false otherwise.
    """

    integrity_demo = fact.select(col("i94addr")).distinct() \
        .join(dim_demographics, fact["i94addr"] == dim_demographics["state_code"], "left_anti") \
        .count() == 0

    integrity_airports = fact.select(col("i94port")).distinct() \
        .join(dim_airports, fact["i94port"] == dim_airports["local_code"], "left_anti") \
        .count() == 0

    integrity_airlines = fact.select(col("airline")).distinct() \
        .join(dim_airlines, fact["airline"] == dim_airlines["IATA"], "left_anti") \
        .count() == 0

    integrity_countries_cit = fact.select(col("i94cit")).distinct() \
        .join(dim_countries, fact["i94cit"] == dim_countries["code"], "left_anti") \
        .count() == 0

    return integrity_demo & integrity_airports & \
           integrity_airlines & integrity_countries_cit & integrity_countries_cit
