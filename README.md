This Python module contains an sample pyspark script which takes 3
parameters(2 input files, and a country list used for filtering data).

Application can be submitted to a Spark cluster (or locally) using 'spark-submit'.
For example, this script can be executed as follows,

spark-submit --py-files <PATH_TO_APP>\dist\bitcoin-0.1.0-py3-none-any.whl 
<PATH_TO_APP>\bitcoin\transformation.py <PATH_TO_FILE>\dataset_one.csv  
<PATH_TO_FILE>\dataset_two.csv "Netherlands, United Kingdom"

The script imports common_functions module to import functions used
to filter data, rename dataframes, check file path, output OS
agnostic paths and logging. Function details can be found in
common_functions module.

The application will output the transformation results in current working
directory under folder client_data. Also, rotating logs can be found in
current working directory