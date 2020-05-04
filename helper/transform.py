"""
This script contains helper functions used for transforming the data in the ETL process.
"""

from pyspark.sql.functions import col, when, lower, isnull, split, udf
from helper.common import convert_to_integer, convert_to_float

# Converts string to Title Case
to_title_case = udf(lambda x: x if x is None else x.title())

def transform_demographics(demographics):
    """
    Transform demographics dataset.
    Conduct two aggegrations to obtain demographic data at the state level.
    Pivot information about race, so that each is presented in a separate column.

    Args:
        demographics (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        demographics (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    int_cols = ['count', 'male_population', 'female_population',
                'total_population', 'number_of_veterans', 'foreign_born']
    float_cols = ['median_age', 'average_household_size']
    demographics = convert_to_integer(demographics, int_cols)
    demographics = convert_to_float(demographics, float_cols)

    first_aggregation = {"median_age": "first",
                         "male_population": "first",
                         "female_population": "first",
                         "total_population": "first",
                         "number_of_veterans": "first",
                         "foreign_born": "first",
                         "average_household_size": "first"}
    aggregate = demographics.groupby(
        ["city", "state", "state_code"]).agg(first_aggregation)
    pivot = demographics.groupBy(
        ["city", "state", "state_code"]).pivot("race").sum("count")

    demographics = aggregate.join(
        other=pivot, on=["city", "state", "state_code"], how="inner")\
        .withColumnRenamed('first(total_population)', 'total_population')\
        .withColumnRenamed('first(female_population)', 'female_population')\
        .withColumnRenamed('first(male_population)', 'male_population')\
        .withColumnRenamed('first(median_age)', 'median_age')\
        .withColumnRenamed('first(number_of_veterans)', 'number_of_veterans')\
        .withColumnRenamed('first(foreign_born)', 'foreign_born')\
        .withColumnRenamed('first(average_household_size)', 'average_household_size')\
        .withColumnRenamed('Hispanic or Latino', 'hispanic_or_latino')\
        .withColumnRenamed('Black or African-American', 'black_or_african_american')\
        .withColumnRenamed('American Indian and Alaska Native', 'american_indian_and_alaska_native')\
        .withColumnRenamed('White', 'white')\
        .withColumnRenamed('Asian', 'asian')

    second_aggregation = {
        "male_population": "sum",
        "female_population": "sum",
        "total_population": "sum",
        "number_of_veterans": "sum",
        "foreign_born": "sum",
        "median_age": "avg",
        "average_household_size": "avg",
        "hispanic_or_latino": "sum",
        "black_or_african_american": "sum",
        "american_indian_and_alaska_native": "sum",
        "white": "sum",
        "asian": "sum"
        }

    # Information about cities will not be required anymore, so let's aggegate it
    state_demographics = demographics.groupby(["state_code", "state"]).agg(second_aggregation)\
                 .withColumnRenamed('sum(male_population)', 'male_population')\
                 .withColumnRenamed('sum(female_population)', 'female_population')\
                 .withColumnRenamed('sum(total_population)', 'total_population')\
                 .withColumnRenamed('sum(number_of_veterans)', 'number_of_veterans')\
                 .withColumnRenamed('sum(foreign_born)', 'foreign_born')\
                 .withColumnRenamed('avg(median_age)', 'median_age')\
                 .withColumnRenamed('avg(average_household_size)', 'average_household_size')\
                 .withColumnRenamed('sum(hispanic_or_latino)', 'hispanic_or_latino')\
                 .withColumnRenamed('sum(black_or_african_american)', 'black_or_african_american')\
                 .withColumnRenamed('sum(american_indian_and_alaska_native)', 'american_indian_and_alaska_native')\
                 .withColumnRenamed('sum(white)', 'white')\
                 .withColumnRenamed('sum(asian)', 'asian')
    return state_demographics

def transform_countries(countries):
    """
    Transform countries dataset.
    Add temporary column with unified case to allow joining.

    Args:
        countries (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        countries (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    countries = countries.withColumn(
        'country_name_lower', lower(countries.country_name))
    return countries

def transform_immigration(immigration):
    """
    Transform immigration dataset.
    Calculate and add date fields related to arrival to be later used in partitioning.

    Args:
        immigration (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        immigration (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    immigration = immigration \
        .withColumn("arrdate_tmp", split(col("arrdate"), "-")) \
        .withColumn("arrival_year", col("arrdate_tmp")[0]) \
        .withColumn("arrival_month", col("arrdate_tmp")[1]) \
        .withColumn("arrival_day", col("arrdate_tmp")[2]) \
        .drop("arrdate_tmp")

    return immigration

def transform_temperature(temperature):
    """
    Transform temperature dataset.
    Aggegate cities data to focus on the country level.
    Tune naming of the countires to allow joining.

    Args:
        temperature (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        temperature (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    temperature = temperature.withColumn(
        'country_name_lower', lower(temperature.Country))
    return temperature

def transform_join_temperature_countries(temperature, countries):
    """
    Join temperature and conutries datasets to form a new countries dataset.
    Remove temporary columns.

    Args:
        temperature (:obj:`SparkDataFrame`): Spark dataframe with temperature dataset
        countries (:obj:`SparkDataFrame`): Spark dataframe with countries dataset
    Returns:
        countries (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    countries = countries.join(temperature, "country_name_lower", how="left")
    countries = countries.withColumn("Country", \
        when(isnull(countries["Country"]), to_title_case(countries.country_name_lower))\
        .otherwise(countries["Country"]))

    drop_columns = ["country_name_lower", "country_name"]
    countries = countries.drop(*drop_columns)
    return countries

def build_facts(immigration, dim_demographics, dim_airports, dim_airlines, dim_countries):
    """
    Transfrom immigration dataset to create fact table.
    Removes rows from the immigration datatset that would not be matched against dimmension tables.

    Args:
        immigration (:obj:`SparkDataFrame`): Spark dataframe with temperature dataset
        dim_demographics (:obj:`SparkDataFrame`): Spark dataframe with demographics dataset
        dim_airlines (:obj:`SparkDataFrame`): Spark dataframe with airlines dataset
        dim_countries (:obj:`SparkDataFrame`): Spark dataframe with countries dataset
    Returns:
        fact (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    facts = immigration \
        .join(dim_demographics, immigration["i94addr"] == dim_demographics["state_code"], "left_semi") \
        .join(dim_airports, immigration["i94port"] == dim_airports["local_code"], "left_semi") \
        .join(dim_airlines, immigration["airline"] == dim_airlines["IATA"], "left_semi") \
        .join(dim_countries, immigration["i94cit"] == dim_countries["code"], "left_semi")
    return facts
