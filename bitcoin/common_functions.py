from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()


def data_filter(df, column_name, value_list):
    """
    :param df: spark dataframe
    :param column_name: column on which filter needs to be applied
    :param value_list: values on which data needs to be filtered
    :return: dataframe with filtered data
    :usage: data_filter(df, 'country', 'United Kingdom')
    """
    formatted_value_list = [x.strip() for x in value_list.split(',')]
    df = df.filter(col(column_name).isin(formatted_value_list))
    return df


def rename_column(df, original_column, new_column):
    """
    :param df: spark dataframe
    :param original_column: list of original names
    :param new_column: list of new names
    :return: dataframe with updated names
    :usage: rename_column(d1, 'country, email', 'Country Name, E-Mail')
    """
    original_column_formatted = [x.strip() for x in original_column.split(',')]
    new_column_formatted = [x.strip() for x in new_column.split(',')]
    mapping = dict(zip(original_column_formatted, new_column_formatted))
    df = df.select([F.col(x).alias(mapping.get(x, x)) for x in original_column_formatted])
    return df


def get_path(file_path):
    """
    :param file_path: OS path of input file
    :return: OS agnostic file path
    """

    formatted_file_path = 'file:///' + file_path.replace('\\', '/')
    return formatted_file_path


def check_file(file_path):
    """
    :param file_path: OS path of input file
    :return: Message if file exists or not
    """
    file_exists = os.path.exists(file_path)
    if file_exists is True:
        file_name = os.path.basename(file_path)
        return "File " + file_name + "Exists!"
    else:
        return "File not found!"
