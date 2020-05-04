# Capstone Project - Udacity Data Engineering

## Evaluation criteria

1. Project code is clean and modular:
   1. [x] All coding scripts have an intuitive, easy-to-follow structure with code separated into logical functions.
   2. [x] Naming for variables and functions follows the PEP8 style guidelines.
   3. [x] The code should run without errors.
2. Quality Checks:
   1. [x] The project includes at least two data quality checks.
3. Data Model:
   1. [x] The ETL processes result in the data model outlined in the write-up.
   2. [x] A data dictionary for the final data model is included.
   3. [x] The data model is appropriate for the identified purpose.
4. Datasets - project includes:
   1. [x] At least 2 data sources
   2. [x] More than 1 million lines of data.
   3. [x] At least two data sources/formats (csv, api, json)

## Preparation for running

Some datasets are not included in this project due to their size.
Before running this project, you need to download the datasets!

Ensure the following files exist at right locations:

- `./data/GlobalLandTemperaturesByCity.csv` from the World Temperature dataset [link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
- `./data/i94_apr16_sub.sas7bdat` I94 Immigration Data: from the US National Tourism and Trade Office [link](https://travel.trade.gov/research/reports/i94/historical/2016.html)

## Running

To start a Docker container with Jupyter Notebook and Spark, run:

```bash
./run_docker.sh
```

Next, open the Jupyter Notebook in the browser (providing the correct access token):

[http://127.0.0.1:8888/notebooks/work/Immigration.ipynb?token=TOKEN](http://127.0.0.1:8888/notebooks/work/Immigration.ipynb?token=TOKEN)

## Resources

It is recommended to assign at least 8GB or memory and >=4 CPU cores for Docker!
