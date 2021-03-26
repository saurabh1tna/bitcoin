from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = os.path.dirname(os.getcwd()).replace('\\', '/')+'/logs/bitcoin.log'

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
    df = df.filter(df[column_name].isin(formatted_value_list))
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
        return "File " + file_name + " exists!"
    else:
        return "File not found!"


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='m', backupCount=5)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())

    logger.propagate = False
    return logger

