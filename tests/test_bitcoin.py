from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType
from bitcoin.common_functions import *
from chispa.dataframe_comparer import *

spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()


def test_filter_check():
    testData = StructType([
        StructField("id", StringType(), True),
        StructField("country", StringType(), False)
    ])
    testRow1 = Row("1", "Netherlands")
    testRow2 = Row("2", "Germany")
    testDf = spark.createDataFrame([testRow1, testRow2], testData)

    expectedRow = Row("1", "Netherlands")
    expectedDF = spark.createDataFrame([expectedRow], testData)

    actualDF = data_filter(testDf, 'country', 'Netherlands')

    assert_df_equality(actualDF, expectedDF)


def test_rename_column_check():
    data1 = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)]
    expectedDf = spark.createDataFrame(data1, ["num", "letter"])
    data2 = [
        (1, "p"),
        (2, "q"),
        (3, "r"),
        (None, None)]
    df2 = spark.createDataFrame(data2, ["num1", "num2"])
    actualDf = rename_column(df2, 'num1,num2', 'num,letter')
    assert_df_equality(actualDf, expectedDf)
