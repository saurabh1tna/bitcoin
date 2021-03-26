"""
transformation.py

This Python module contains an sample pyspark script which takes 3
parameters(2 input files and a country list as filter). It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'.
For example, this script can be executed as follows,
    $SPARK_HOME/bin/spark-submit \
    --py-files <PATH>\bitcoin\dist\bitcoin-0.1.0-py3-none-any.whl \
    <PATH>\bitcoin\bitcoin\transformation.py \
    C:\<PATH>\dataset_one.csv \
    C:\<PATH>\dataset_two.csv \
    "Netherlands, United Kingdom"
The script imports common_functions module to import functions used
to filter data, rename dataframes, check file path, output OS
agnostic paths and logging. Function details can be found in
common_functions module.
"""

from common_functions import *
import argparse

"""Initialising logging"""
my_logger = get_logger("Bitcoin Transformation")
my_logger.info("Application Started!")

parser = argparse.ArgumentParser()
parser.add_argument("dataset1", help="Enter path for dataset 1")
parser.add_argument("dataset2", help="Enter path for dataset 2")
parser.add_argument("country_list", help="Enter country list")
args = parser.parse_args()
my_logger.info("Dataset1: "+args.dataset1)
my_logger.info("Dataset2: "+args.dataset2)
my_logger.info("Country list: "+args.country_list)


spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()

dataset1_path = args.dataset1
check_file(dataset1_path)
my_logger.info(check_file(dataset1_path))

dataset2_path = args.dataset2
check_file(dataset2_path)
my_logger.info(check_file(dataset2_path))

country_list = args.country_list

d1 = spark.read.csv(get_path(dataset1_path), inferSchema='true', header='true')
d1_filtered = data_filter(d1, 'country', country_list)

d1_select = rename_column(d1_filtered, 'id, email, country', 'id1, email, country')

d2 = spark.read.csv(get_path(dataset2_path), inferSchema='true', header='true')

d2_select = rename_column(d2, 'id, btc_a, cc_t', 'id, btc_a, cc_t')

join_expression = d1_select["id1"] == d2_select["id"]
joinType = "inner"


result = d1_select.join(d2_select, join_expression, joinType)

formatted_results = rename_column(result, 'id, btc_a, cc_t, email, country', 'client_identifier, bitcoin_address, credit_card_type, email, country')
target_dir = 'client_data'
formatted_results.write.format("csv").mode("overwrite").option("path", target_dir).option("header", "true").save()

my_logger.info("Application completed. Check output file at path "+target_dir)

print("Application completed. Check output file at path "+os.getcwd().replace('\\', '/')+"/"+target_dir)
print("Log file at path "+os.getcwd().replace('\\', '/')+"/bitcoin.log")