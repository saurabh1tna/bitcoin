from common_functions import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("dataset1", help="Enter path for dataset 1")
parser.add_argument("dataset2", help="Enter path for dataset 2")
parser.add_argument("country_list", help="Enter country list")
args = parser.parse_args()
print(args.dataset1)


spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()

dataset1_path = args.dataset1
check_file(dataset1_path)
dataset2_path = args.dataset2
check_file(dataset2_path)

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
target = 'client_data'
target_dir = os.path.dirname(os.getcwd()).replace('\\', '/')+'/client_data'
formatted_results.write.format("csv").mode("overwrite").option("path", target_dir).option("header", "true").save()
