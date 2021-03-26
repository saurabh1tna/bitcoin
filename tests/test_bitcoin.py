from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType
from bitcoin.common_functions import *
from chispa.dataframe_comparer import *

spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()


def filter_check():
    testData = StructType([
        StructField("id", StringType(), True),
        StructField("country", StringType(), False)
    ])
    testRow1 = Row("1", "Netherlands")
    testRow2 = Row("2", "Germany")
    testDf = spark.createDataFrame([testRow1, testRow2], testData)

    expectedRow = Row("1", "Netherlands")
    expectedDF = spark.createDataFrame([expectedRow], testData)

    actualDF = data_filter(testDf, 'country', 'Netherlands, Germany')

    assert_df_equality(actualDF, expectedDF)


filter_check()
