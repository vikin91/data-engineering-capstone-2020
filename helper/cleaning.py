"""
This script contains helper functionf for cleaning the datasets used in this project.
"""

from pyspark.sql.functions import col, lower, substring
from helper.common import convert_to_integer, convert_to_float, convert_to_date, rename_field


def clean_demographics(demographics):
    """
    Clean demographics dataset.
    Set correct formats in numbers and dates and select only important columns.
    Fill null values with zeros in numeric fields.

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
    demographics = demographics.fillna(0, [*int_cols, *float_cols])
    return demographics


def clean_airports(airports):
    """
    Clean airports dataset.
    Filter-out null values in fields designed to be FK.
    Set correct formats in numbers and dates and select only important columns.

    Args:
        airports (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        airports (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    airports = airports.where(
        (col("iso_country") == "US") &
        col("type").isin("large_airport", "medium_airport", "small_airport") &
        col("local_code").isNotNull())\
        .withColumn("iso_region", substring(col("iso_region"), 4, 2))\
        .withColumn("elevation_ft", col("elevation_ft").cast("float"))

    return airports


def clean_immigration(immigration):
    """
    Clean immigration dataset.
    Rename columns to remove white spaces.
    Set correct formats in numbers and dates and select only important columns.
    In particular remove the following columns: 'admnum', 'biryear', 'count',
    'dtaddto', 'dtadfile', 'entdepa', 'entdepd', 'entdepu', 'insnum', 'matflag', 'occup';

    Args:
        immigration (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        immigration (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    drop_columns = ["visapost", "occup", "entdepu", "insnum", "count",
                    "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum"]
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',
                'arrdate', 'i94mode', 'i94bir', 'i94visa', 'count',
                'biryear', 'dtadfile', 'depdate']
    date_cols = ['arrdate', 'depdate']

    immigration = immigration.drop(*drop_columns)
    immigration = convert_to_integer(immigration, int_cols)
    immigration = convert_to_date(immigration, date_cols)

    # Fields where i94addr==null would be dropped by semi-left joining anyways
    immigration = immigration.where(col("i94addr").isNotNull())
    return immigration


def clean_airlines(airlines):
    """
    Clean airlines dataset.
    Filter-out null values in fields designed to be FK.

    Args:
        airlines (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        airlines (:obj:`SparkDataFrame`): Processed Spark dataframe
    """
    airlines = airlines \
        .where((col("IATA").isNotNull()) & (col("Airline_ID") > 1)) \
        .drop("Alias")

    return airlines


def clean_temperature(temperature):
    """
    Clean temperature dataset.
    Rename skewed values in fields designed to be FK.
    Aggregate cities information to focus on the countries level.

    Args:
        temperature (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        temperature (:obj:`SparkDataFrame`): Processed Spark dataframe
    """
    temperature = temperature.groupby(["Country"]).agg(
        {"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
        .withColumnRenamed('avg(AverageTemperature)', 'avg_temperature')\
        .withColumnRenamed('first(Latitude)', 'latitude')\
        .withColumnRenamed('first(Longitude)', 'longitude')

    change_countries = [
        ("Congo (Democratic Republic Of The)", "Congo"),
        ("Côte D'Ivoire", "Ivory Coast")
    ]
    temperature = rename_field(temperature, "Country", change_countries)

    temperature = temperature.withColumn(
        'country_name_lower', lower(temperature.Country))
    return temperature


def clean_countries(countries):
    """
    Clean countries dataset.
    Rename skewed values in fields designed to be FK.
    Set correct formats in numbers-fileds.

    Args:
        countries (:obj:`SparkDataFrame`): Spark dataframe to be processed.
    Returns:
        countries (:obj:`SparkDataFrame`): Processed Spark dataframe
    """

    # Rename specific country names to match the demographics dataset when joining them
    change_countries = [("MEXICO Air Sea, and Not Reported (I-94, no land arrivals)", "MEXICO"),
                        ("BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"),
                        ("INVALID: CANADA", "CANADA"),
                        ("CHINA, PRC", "CHINA"),
                        ("GUINEA-BISSAU", "GUINEA BISSAU"),
                        ("INVALID: PUERTO RICO", "PUERTO RICO"),
                        ("INVALID: UNITED STATES", "UNITED STATES")]

    countries = convert_to_integer(countries, ["Code"])
    countries = rename_field(countries, "country_name", change_countries)
    return countries
